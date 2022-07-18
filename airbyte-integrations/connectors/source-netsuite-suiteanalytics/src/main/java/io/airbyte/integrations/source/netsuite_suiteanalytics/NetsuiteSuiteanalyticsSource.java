/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.netsuite_suiteanalytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.db.jdbc.streaming.AdaptiveStreamingQueryConfig;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource;
import java.sql.JDBCType;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetsuiteSuiteanalyticsSource extends AbstractJdbcSource<JDBCType> implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(NetsuiteSuiteanalyticsSource.class);

  static final String DRIVER_CLASS = "com.netsuite.jdbc.openaccess.OpenAccessDriver";

  public NetsuiteSuiteanalyticsSource() {
    // if the JDBC driver does not support custom fetch size, use NoOpStreamingQueryConfig
    // instead of AdaptiveStreamingQueryConfig.
    super(DRIVER_CLASS, AdaptiveStreamingQueryConfig::new, JdbcUtils.getDefaultSourceOperations());
  }

  // The config is based on spec.json, update according to your DB
  @Override
  public JsonNode toDatabaseConfig(final JsonNode config) {
    // create DB config. Ex: "Jsons.jsonNode(ImmutableMap.builder().put("username",
    // userName).put("password", pas)...build());
    final StringBuilder jdbcUrl = new StringBuilder(String.format("jdbc:ns://%s:%s;ServerDataSource=%s;Encrypted=%s;NegotiateSSLClose=%s;CustomProperties=(AccountID=%s;RoleID=%s)",
      config.get("host").asText(),
      config.get("port").asText(),
      config.get("server_data_source").asText(),
      config.get("encrypted").asText(),
      config.get("negotiate_ssl_close").asText(),
      config.get("account_id").asText(),
      config.get("role_id").asText()));

    final ImmutableMap.Builder<Object, Object> configBuilder = ImmutableMap.builder()
      .put("username", config.get("username").asText())
      .put("jdbc_url", jdbcUrl.toString());

    if (config.has("password")) {
      configBuilder.put("password", config.get("password").asText());
    }

    return Jsons.jsonNode(configBuilder.build());
  }

  @Override
  public Set<String> getExcludedInternalNameSpaces() {
    // Add tables to exclude, Ex "INFORMATION_SCHEMA", "sys", "spt_fallback_db", etc
    return Set.of("");
  }

  public static void main(final String[] args) throws Exception {
    final Source source = new NetsuiteSuiteanalyticsSource();
    LOGGER.info("starting source: {}", NetsuiteSuiteanalyticsSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("completed source: {}", NetsuiteSuiteanalyticsSource.class);
  }

}
