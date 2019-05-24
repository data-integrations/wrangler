/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.service.database;

import com.google.common.base.Throwables;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.Row;
import org.hsqldb.Server;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Class description here.
 */
public class DatabaseHandlerTest {
  private static HSQLDBServer hsqlDBServer;
  private static final long CURRENT_TS = System.currentTimeMillis();

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUp() throws Exception {
    String hsqlDBDir = temporaryFolder.newFolder("hsqldb").getAbsolutePath();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    hsqlDBServer = new HSQLDBServer(hsqlDBDir, "testdb");
    hsqlDBServer.start();
    try (Connection conn = hsqlDBServer.getConnection()) {
      createTestUser(conn);
      createTestTables(conn);
      prepareTestData(conn);
    }
  }

  @AfterClass
  public static void tearDownDB() throws SQLException {
    try (Connection conn = hsqlDBServer.getConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE \"my_table\"");
    }
    hsqlDBServer.stop();
  }


  @Test
  public void testReadingDriverConfiguration() throws Exception {
    Map<String, DatabaseHandler.DriverInfo> drivers = new HashMap<>();
    int expectedDrivers = 12;
    try (InputStream is = DatabaseHandler.class.getClassLoader().getResourceAsStream("drivers.mapping")) {
      DatabaseHandler.loadDrivers(is, drivers);
      JsonArray values = new JsonArray();
      Collection<Map.Entry<String, DatabaseHandler.DriverInfo>> entries = drivers.entrySet();
      for (Map.Entry<String, DatabaseHandler.DriverInfo> driver : entries) {
        JsonObject object = new JsonObject();
        object.addProperty("class", driver.getKey());
        object.addProperty("label", driver.getValue().getName());
        String shortTag = driver.getValue().getTag();
        object.addProperty("tag", shortTag);
        object.addProperty("name", shortTag);
        object.addProperty("default.port", driver.getValue().getPort());
        values.add(object);
      }
      Assert.assertEquals(expectedDrivers, values.size());
    }
    Assert.assertEquals(expectedDrivers, drivers.size());
  }

  @Test
  public void databaseWithLogicalTypes() throws Exception {
    List<Row> expected = new ArrayList<>();
    Row row = new Row();
    row.add("ID", 1);
    row.add("NAME", "alice");
    row.add("DATE_COL", new Date(CURRENT_TS).toLocalDate());
    row.add("TIME_COL", new Time(CURRENT_TS).toLocalTime());
    row.add("TIMESTAMP_COL", new Timestamp(CURRENT_TS).toInstant().atZone(ZoneId.of("UTC")));
    expected.add(row);
    row = new Row();
    row.add("ID", 2);
    row.add("NAME", "bob");
    row.add("DATE_COL", new Date(CURRENT_TS).toLocalDate());
    row.add("TIME_COL", null);
    row.add("TIMESTAMP_COL", null);
    expected.add(row);

    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute("SELECT * FROM \"my_table\"");
      try (ResultSet resultSet = stmt.getResultSet()) {
        List<Row> actual = DatabaseHandler.getRows(2, resultSet);
        Assert.assertEquals(expected.get(0).getValue(0), actual.get(0).getValue(0));
        Assert.assertEquals(expected.get(0).getValue(1), actual.get(0).getValue(1));
        Assert.assertEquals(expected.get(0).getValue(2), actual.get(0).getValue(2));
        Assert.assertEquals(expected.get(0).getValue(3), actual.get(0).getValue(3));
        Assert.assertEquals(expected.get(0).getValue(4), actual.get(0).getValue(4));

        Assert.assertEquals(expected.get(1).getValue(0), actual.get(1).getValue(0));
        Assert.assertEquals(expected.get(1).getValue(1), actual.get(1).getValue(1));
        Assert.assertEquals(expected.get(1).getValue(2), actual.get(1).getValue(2));
        Assert.assertEquals(expected.get(1).getValue(3), actual.get(1).getValue(3));
        Assert.assertEquals(expected.get(1).getValue(4), actual.get(1).getValue(4));
      }
    }
  }

  private static void createTestUser(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE USER \"emptyPwdUser\" PASSWORD '' ADMIN");
    }
  }

  private static void createTestTables(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      // note that the tables need quotation marks around them; otherwise, hsql creates them in upper case
      stmt.execute("CREATE TABLE \"my_table\"" +
                     "(" +
                     "ID INT NOT NULL, " +
                     "NAME VARCHAR(40) NOT NULL, " +
                     "DATE_COL DATE, " +
                     "TIME_COL TIME, " +
                     "TIMESTAMP_COL TIMESTAMP, " +
                     ")");
    }
  }

  private static void prepareTestData(Connection conn) throws SQLException {
    try (
      PreparedStatement pStmt1 =
        conn.prepareStatement("INSERT INTO \"my_table\" VALUES(?, ?, ?, ?, ?)")) {
      pStmt1.setInt(1, 1);
      pStmt1.setString(2, "alice");
      pStmt1.setDate(3, new Date(CURRENT_TS));
      pStmt1.setTime(4, new Time(CURRENT_TS));
      pStmt1.setTimestamp(5, new Timestamp(CURRENT_TS));
      pStmt1.executeUpdate();

      pStmt1.setInt(1, 2);
      pStmt1.setString(2, "bob");
      pStmt1.setDate(3, new Date(CURRENT_TS));
      pStmt1.setTime(4, null);
      pStmt1.setTimestamp(5, null);
      pStmt1.executeUpdate();
    }
  }

  private Connection getConnection() {
    return hsqlDBServer == null ? null : hsqlDBServer.getConnection();
  }

  private static class HSQLDBServer {
    private final String locationUrl;
    private final String database;
    private final String connectionUrl;
    private final Server server;
    private final String hsqlDBDriver = "org.hsqldb.jdbcDriver";

    private HSQLDBServer(String location, String database) {
      this.locationUrl = String.format("%s/%s", location, database);
      this.database = database;
      this.connectionUrl = String.format("jdbc:hsqldb:hsql://localhost/%s", database);
      this.server = new Server();
    }

    void start() {
      server.setDatabasePath(0, locationUrl);
      server.setDatabaseName(0, database);
      server.start();
    }

    void stop() {
      server.stop();
    }

    Connection getConnection() {
      try {
        Class.forName(hsqlDBDriver);
        return DriverManager.getConnection(connectionUrl);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
