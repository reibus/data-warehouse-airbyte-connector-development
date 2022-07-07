/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres;

import static io.airbyte.integrations.debezium.AirbyteDebeziumHandler.shouldUseCDC;
import static io.airbyte.integrations.debezium.internals.DebeziumEventUtils.CDC_DELETED_AT;
import static io.airbyte.integrations.debezium.internals.DebeziumEventUtils.CDC_UPDATED_AT;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BINARY;
import static java.sql.JDBCType.BIT;
import static java.sql.JDBCType.BLOB;
import static java.sql.JDBCType.BOOLEAN;
import static java.sql.JDBCType.CHAR;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.FLOAT;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.LONGVARCHAR;
import static java.sql.JDBCType.NCHAR;
import static java.sql.JDBCType.NUMERIC;
import static java.sql.JDBCType.NVARCHAR;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TIME;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.JDBCType.TIME_WITH_TIMEZONE;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARCHAR;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Sets;
import com.google.common.collect.Lists;
import io.airbyte.commons.functional.CheckedConsumer;
import io.airbyte.commons.functional.CheckedFunction;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.db.factory.DatabaseDriver;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.streaming.AdaptiveStreamingQueryConfig;
import io.airbyte.integrations.base.AirbyteStreamNameNamespacePair;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.base.ssh.SshWrappedSource;
import io.airbyte.integrations.debezium.AirbyteDebeziumHandler;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource;
import io.airbyte.integrations.source.jdbc.dto.JdbcPrivilegeDto;
import io.airbyte.integrations.source.relationaldb.TableInfo;
import io.airbyte.integrations.source.relationaldb.models.CdcState;
import io.airbyte.integrations.source.relationaldb.state.StateManager;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteGlobalState;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.CommonField;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.SyncMode;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresSource extends AbstractJdbcSource<JDBCType> implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSource.class);
  public static final String CDC_LSN = "_ab_cdc_lsn";

  static final String DRIVER_CLASS = DatabaseDriver.POSTGRESQL.getDriverClassName();
  private final Set<JDBCType> allowedCursorTypes = Set.of(TIMESTAMP, TIMESTAMP_WITH_TIMEZONE, TIME, TIME_WITH_TIMEZONE,
          DATE, BIT, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, REAL, NUMERIC, DECIMAL,
          CHAR, NCHAR, NVARCHAR, VARCHAR, LONGVARCHAR, BINARY, BLOB);
  private List<String> schemas;

  public static Source sshWrappedSource() {
    return new SshWrappedSource(new PostgresSource(), List.of("host"), List.of("port"));
  }

  PostgresSource() {
    super(DRIVER_CLASS, AdaptiveStreamingQueryConfig::new, new PostgresSourceOperations());
  }

  @Override
  public JsonNode toDatabaseConfig(final JsonNode config) {
    return toDatabaseConfigStatic(config);
  }

  // todo (cgardens) - restructure AbstractJdbcSource so to take this function in the constructor. the
  // current structure forces us to declarehave a bunch of pure function methods as instance members
  // when they could be static.
  public JsonNode toDatabaseConfigStatic(final JsonNode config) {
    final List<String> additionalParameters = new ArrayList<>();

    final StringBuilder jdbcUrl = new StringBuilder(String.format("jdbc:postgresql://%s:%s/%s?",
        config.get("host").asText(),
        config.get("port").asText(),
        config.get("database").asText()));

    if (config.get("jdbc_url_params") != null && !config.get("jdbc_url_params").asText().isEmpty()) {
      jdbcUrl.append(config.get("jdbc_url_params").asText()).append("&");
    }

    // assume ssl if not explicitly mentioned.
    if (!config.has("ssl") || config.get("ssl").asBoolean()) {
      additionalParameters.add("ssl=true");
      additionalParameters.add("sslmode=require");
    }

    if (config.has("schemas") && config.get("schemas").isArray()) {
      schemas = new ArrayList<>();
      for (final JsonNode schema : config.get("schemas")) {
        schemas.add(schema.asText());
      }
    }

    if (schemas != null && !schemas.isEmpty()) {
      additionalParameters.add("currentSchema=" + String.join(",", schemas));
    }

    additionalParameters.forEach(x -> jdbcUrl.append(x).append("&"));

    final Builder<Object, Object> configBuilder = ImmutableMap.builder()
        .put("username", config.get("username").asText())
        .put("jdbc_url", jdbcUrl.toString());

    if (config.has("password")) {
      configBuilder.put("password", config.get("password").asText());
    }

    return Jsons.jsonNode(configBuilder.build());
  }

  @Override
  protected List<String> getCursorFields(List<CommonField<JDBCType>> fields) {
    return fields.stream()
            .filter(field -> allowedCursorTypes.contains(JDBCType.valueOf(field.getType().toString())))
            .map(field -> field.getName())
            .collect(Collectors.toList());
  }


  @Override
  public Set<String> getExcludedInternalNameSpaces() {
    return Set.of("information_schema", "pg_catalog", "pg_internal", "catalog_history");
  }

  @Override
  public AirbyteCatalog discover(final JsonNode config) throws Exception {
    final AirbyteCatalog catalog = super.discover(config);

    if (isCdc(config)) {
      final List<AirbyteStream> streams = catalog.getStreams().stream()
          .map(PostgresSource::overrideSyncModes)
          .map(PostgresSource::removeIncrementalWithoutPk)
          .map(PostgresSource::setIncrementalToSourceDefined)
          .map(PostgresSource::addCdcMetadataColumns)
          .collect(toList());

      catalog.setStreams(streams);
    }

    return catalog;
  }

  @Override
  public List<TableInfo<CommonField<JDBCType>>> discoverInternal(final JdbcDatabase database) throws Exception {
    if (schemas != null && !schemas.isEmpty()) {
      // process explicitly selected (from UI) schemas
      final List<TableInfo<CommonField<JDBCType>>> internals = new ArrayList<>();
      for (final String schema : schemas) {
        LOGGER.info("Checking schema: {}", schema);
        final List<TableInfo<CommonField<JDBCType>>> tables = super.discoverInternal(database, schema);
        internals.addAll(tables);
        for (final TableInfo<CommonField<JDBCType>> table : tables) {
          LOGGER.info("Found table: {}.{}", table.getNameSpace(), table.getName());
        }
      }
      return internals;
    } else {
      LOGGER.info("No schemas explicitly set on UI to process, so will process all of existing schemas in DB");
      return super.discoverInternal(database);
    }
  }

  @Override
  public List<CheckedConsumer<JdbcDatabase, Exception>> getCheckOperations(final JsonNode config)
      throws Exception {
    final List<CheckedConsumer<JdbcDatabase, Exception>> checkOperations = new ArrayList<>(
        super.getCheckOperations(config));

    if (isCdc(config)) {
      checkOperations.add(database -> {
        final List<JsonNode> matchingSlots = database.queryJsons(connection -> {
          final String sql = "SELECT * FROM pg_replication_slots WHERE slot_name = ? AND plugin = ? AND database = ?";
          final PreparedStatement ps = connection.prepareStatement(sql);
          ps.setString(1, config.get("replication_method").get("replication_slot").asText());
          ps.setString(2, PostgresUtils.getPluginValue(config.get("replication_method")));
          ps.setString(3, config.get("database").asText());

          LOGGER.info(
              "Attempting to find the named replication slot using the query: " + ps.toString());

          return ps;
        }, sourceOperations::rowToJson);

        if (matchingSlots.size() != 1) {
          throw new RuntimeException(
              "Expected exactly one replication slot but found " + matchingSlots.size()
                  + ". Please read the docs and add a replication slot to your database.");
        }

      });

      checkOperations.add(database -> {
        final List<JsonNode> matchingPublications = database.queryJsons(connection -> {
          final PreparedStatement ps = connection.prepareStatement("SELECT * FROM pg_publication WHERE pubname = ?");
          ps.setString(1, config.get("replication_method").get("publication").asText());
          LOGGER.info("Attempting to find the publication using the query: " + ps);
          return ps;
        }, sourceOperations::rowToJson);

        if (matchingPublications.size() != 1) {
          throw new RuntimeException(
              "Expected exactly one publication but found " + matchingPublications.size()
                  + ". Please read the docs and add a publication to your database.");
        }

      });
    }

    return checkOperations;
  }

  @Override
  public AutoCloseableIterator<AirbyteMessage> read(final JsonNode config,
                                                    final ConfiguredAirbyteCatalog catalog,
                                                    final JsonNode state)
      throws Exception {
    // this check is used to ensure that have the pgoutput slot available so Debezium won't attempt to
    // create it.
    final AirbyteConnectionStatus check = check(config);

    if (check.getStatus().equals(Status.FAILED)) {
      throw new RuntimeException("Unable establish a connection: " + check.getMessage());
    }

    return super.read(config, catalog, state);
  }

  @Override
  public List<AutoCloseableIterator<AirbyteMessage>> getIncrementalIterators(
                                                                             final JdbcDatabase database,
                                                                             final ConfiguredAirbyteCatalog catalog,
                                                                             final Map<String, TableInfo<CommonField<JDBCType>>> tableNameToTable,
                                                                             final StateManager stateManager,
                                                                             final Instant emittedAt) {
    final JsonNode sourceConfig = database.getSourceConfig();
    if (isCdc(sourceConfig) && shouldUseCDC(catalog)) {
      final AirbyteDebeziumHandler handler = new AirbyteDebeziumHandler(sourceConfig,
          PostgresCdcTargetPosition.targetPosition(database), false);
      final PostgresCdcStateHandler postgresCdcStateHandler = new PostgresCdcStateHandler(stateManager);
      final List<ConfiguredAirbyteStream> streamsToSnapshot = identifyStreamsToSnapshot(catalog, stateManager);
      final Supplier<AutoCloseableIterator<AirbyteMessage>> incrementalIteratorSupplier = () -> handler.getIncrementalIterators(catalog,
          new PostgresCdcSavedInfoFetcher(stateManager.getCdcStateManager().getCdcState()),
          postgresCdcStateHandler,
          new PostgresCdcConnectorMetadataInjector(),
          PostgresCdcProperties.getDebeziumDefaultProperties(sourceConfig),
          emittedAt);
      if (streamsToSnapshot.isEmpty()) {
        return Collections.singletonList(incrementalIteratorSupplier.get());
      }

      final AutoCloseableIterator<AirbyteMessage> snapshotIterator = handler.getSnapshotIterators(
          new ConfiguredAirbyteCatalog().withStreams(streamsToSnapshot), new PostgresCdcConnectorMetadataInjector(),
          PostgresCdcProperties.getSnapshotProperties(), postgresCdcStateHandler, emittedAt);
      return Collections.singletonList(AutoCloseableIterators.concatWithEagerClose(snapshotIterator, AutoCloseableIterators.lazyIterator(incrementalIteratorSupplier)));

    } else {
      return super.getIncrementalIterators(database, catalog, tableNameToTable, stateManager, emittedAt);
    }
  }

  protected List<ConfiguredAirbyteStream> identifyStreamsToSnapshot(final ConfiguredAirbyteCatalog catalog, final StateManager stateManager) {
    final Set<AirbyteStreamNameNamespacePair> alreadySyncedStreams = stateManager.getCdcStateManager().getInitialStreamsSynced();
    if (alreadySyncedStreams.isEmpty() && (stateManager.getCdcStateManager().getCdcState() == null
        || stateManager.getCdcStateManager().getCdcState().getState() == null)) {
      return Collections.emptyList();
    }

    final Set<AirbyteStreamNameNamespacePair> allStreams = AirbyteStreamNameNamespacePair.fromConfiguredCatalog(catalog);

    final Set<AirbyteStreamNameNamespacePair> newlyAddedStreams = new HashSet<>(Sets.difference(allStreams, alreadySyncedStreams));

    return catalog.getStreams().stream()
        .filter(stream -> newlyAddedStreams.contains(AirbyteStreamNameNamespacePair.fromAirbyteSteam(stream.getStream())))
        .map(Jsons::clone)
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  static boolean isCdc(final JsonNode config) {
    final boolean isCdc = config.hasNonNull("replication_method")
        && config.get("replication_method").hasNonNull("replication_slot")
        && config.get("replication_method").hasNonNull("publication");
    LOGGER.info("using CDC: {}", isCdc);
    return isCdc;
  }

  private static AirbyteStream overrideSyncModes(final AirbyteStream stream) {
    return stream.withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
  }

  /*
   * It isn't possible to recreate the state of the original database unless we include extra
   * information (like an oid) when using logical replication. By limiting to Full Refresh when we
   * don't have a primary key we dodge the problem for now. As a work around a CDC and non-CDC source
   * could be configured if there's a need to replicate a large non-PK table.
   *
   * Note: in place mutation.
   */
  private static AirbyteStream removeIncrementalWithoutPk(final AirbyteStream stream) {
    if (stream.getSourceDefinedPrimaryKey().isEmpty()) {
      stream.getSupportedSyncModes().remove(SyncMode.INCREMENTAL);
    }

    return stream;
  }

  @Override
  public Set<JdbcPrivilegeDto> getPrivilegesTableForCurrentUser(final JdbcDatabase database,
                                                                final String schema)
      throws SQLException {
    final CheckedFunction<Connection, PreparedStatement, SQLException> statementCreator = connection -> {
      final PreparedStatement ps = connection.prepareStatement(
          """
                 SELECT nspname as table_schema,
                        relname as table_name
                 FROM   pg_class c
                 JOIN   pg_namespace n on c.relnamespace = n.oid
                 WHERE  has_table_privilege(c.oid, 'SELECT')
                 -- r = ordinary table, i = index, S = sequence, t = TOAST table, v = view, m = materialized view, c = composite type, f = foreign table, p = partitioned table, I = partitioned index
                 AND    relkind in ('r', 'm', 'v', 't', 'f', 'p')
                 and    ((? is null) OR nspname = ?)
          """);
      ps.setString(1, schema);
      ps.setString(2, schema);
      return ps;
    };

    return database.queryJsons(statementCreator, sourceOperations::rowToJson)
        .stream()
        .map(e -> JdbcPrivilegeDto.builder()
            .schemaName(e.get("table_schema").asText())
            .tableName(e.get("table_name").asText())
            .build())
        .collect(toSet());
  }

  @VisibleForTesting
  static String getUsername(final JsonNode databaseConfig) {
    final String jdbcUrl = databaseConfig.get("jdbc_url").asText();
    final String username = databaseConfig.get("username").asText();

    // Azure Postgres server has this username pattern: <username>@<host>.
    // Inside Postgres, the true username is just <username>.
    // The jdbc_url is constructed in the toDatabaseConfigStatic method.
    if (username.contains("@") && jdbcUrl.contains("azure.com:")) {
      final String[] tokens = username.split("@");
      final String postgresUsername = tokens[0];
      LOGGER.info("Azure username \"{}\" is detected; use \"{}\" to check permission", username, postgresUsername);
      return postgresUsername;
    }

    return username;
  }

  @Override
  protected boolean isNotInternalSchema(final JsonNode jsonNode, final Set<String> internalSchemas) {
    return false;
  }

  /*
   * Set all streams that do have incremental to sourceDefined, so that the user cannot set or
   * override a cursor field.
   *
   * Note: in place mutation.
   */
  private static AirbyteStream setIncrementalToSourceDefined(final AirbyteStream stream) {
    if (stream.getSupportedSyncModes().contains(SyncMode.INCREMENTAL)) {
      stream.setSourceDefinedCursor(true);
    }

    return stream;
  }

  // Note: in place mutation.
  private static AirbyteStream addCdcMetadataColumns(final AirbyteStream stream) {
    final ObjectNode jsonSchema = (ObjectNode) stream.getJsonSchema();
    final ObjectNode properties = (ObjectNode) jsonSchema.get("properties");

    final JsonNode stringType = Jsons.jsonNode(ImmutableMap.of("type", "string"));
    final JsonNode numberType = Jsons.jsonNode(ImmutableMap.of("type", "number"));
    properties.set(CDC_LSN, numberType);
    properties.set(CDC_UPDATED_AT, stringType);
    properties.set(CDC_DELETED_AT, stringType);

    return stream;
  }

  // TODO This is a temporary override so that the Postgres source can take advantage of per-stream
  // state
  @Override
  protected List<AirbyteStateMessage> generateEmptyInitialState(final JsonNode config) {
    if (getSupportedStateType(config) == AirbyteStateType.GLOBAL) {
      final AirbyteGlobalState globalState = new AirbyteGlobalState()
          .withSharedState(Jsons.jsonNode(new CdcState()))
          .withStreamStates(List.of());
      return List.of(new AirbyteStateMessage().withType(AirbyteStateType.GLOBAL).withGlobal(globalState));
    } else {
      return List.of(new AirbyteStateMessage()
          .withType(AirbyteStateType.STREAM)
          .withStream(new AirbyteStreamState()));
    }
  }

  @Override
  protected AirbyteStateType getSupportedStateType(final JsonNode config) {
    return isCdc(config) ? AirbyteStateType.GLOBAL : AirbyteStateType.STREAM;
  }

  public static void main(final String[] args) throws Exception {
    final Source source = PostgresSource.sshWrappedSource();
    LOGGER.info("starting source: {}", PostgresSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("completed source: {}", PostgresSource.class);
  }

}
