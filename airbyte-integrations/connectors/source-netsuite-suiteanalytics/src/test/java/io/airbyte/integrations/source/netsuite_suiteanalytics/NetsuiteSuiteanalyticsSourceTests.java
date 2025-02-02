/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.netsuite_suiteanalytics;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.db.Database;
import org.junit.jupiter.api.Test;

public class NetsuiteSuiteanalyticsSourceTests {

  private JsonNode config;
  private Database database;

  @Test
  public void testSettingTimezones() throws Exception {
    // TODO init your container. Ex: "new
    // org.testcontainers.containers.MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2019-latest").acceptLicense();"
    // TODO start the container. Ex: "container.start();"
    // TODO prepare DB config. Ex: "config = getConfig(container, dbName,
    // "serverTimezone=Europe/London");"
    // TODO create DB, grant all privileges, etc.
    // TODO check connection status. Ex: "AirbyteConnectionStatus check = new
    // ScaffoldJavaJdbcGenericSource().check(config);"
    // TODO assert connection status. Ex: "assertEquals(AirbyteConnectionStatus.Status.SUCCEEDED,
    // check.getStatus());"
    // TODO cleanup used resources and close used container. Ex: "container.close();"
  }

}
