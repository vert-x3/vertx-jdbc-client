/*
 * Copyright (c) 2011-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.it;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.jdbcclient.SqlOutParam;
import io.vertx.sqlclient.*;
import org.junit.*;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.sql.JDBCType;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.testcontainers.containers.BindMode.READ_ONLY;

public class MSSQLTest {

  private static MSSQLServer server;

  @BeforeClass
  public static void setup() throws Exception {
    server = new MSSQLServer();
    server.start();
  }

  @AfterClass
  public static void tearDown() {
    server.stop();
  }

  private Vertx vertx;

  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  @After
  public void after() throws Exception {
    vertx
      .close()
      .await(20, TimeUnit.SECONDS);
  }

  static class MSSQLServer {

    private static final String USER = "SA";
    private static final String PASSWORD = "A_Str0ng_Required_Password";
    private static final String INIT_SQL = "/opt/data/init.sql";

    private GenericContainer genericContainer;

    public void start() throws Exception {
      String containerVersion = "2019-latest";
      genericContainer = new GenericContainer<>("mcr.microsoft.com/mssql/server:" + containerVersion)
        .withLogConsumer(fr -> {
          System.out.print("MSSQL: " + fr.getUtf8String());
        })
        .withEnv("ACCEPT_EULA", "Y")
        .withEnv("TZ", "UTC")
        .withEnv("SA_PASSWORD", PASSWORD)
        .withExposedPorts(MSSQLServerContainer.MS_SQL_SERVER_PORT)
        .withClasspathResourceMapping("init-mssql.sql", INIT_SQL, READ_ONLY)
        .waitingFor(Wait.forLogMessage(".*The tempdb database has \\d+ data file\\(s\\).*\\n", 2));
      genericContainer.start();
      initDb();
    }

    public int getPort() {
      return genericContainer.getMappedPort(MSSQLServerContainer.MS_SQL_SERVER_PORT);
    }

    public String getUsername() {
      return USER;
    }

    public String getPassword() {
      return PASSWORD;
    }

    public void stop() {
      genericContainer.stop();
    }

    private void initDb() throws IOException {
      try {
        Container.ExecResult execResult = genericContainer.execInContainer(
          "/opt/mssql-tools18/bin/sqlcmd",
          "-S", "localhost",
          "-U", USER,
          "-P", PASSWORD,
          "-i", INIT_SQL,
          "-C", "-No"
        );
        System.out.println("Init stdout: " + execResult.getStdout());
        System.out.println("Init stderr: " + execResult.getStderr());
        if (execResult.getExitCode() != 0) {
          throw new RuntimeException(String.format("Failure while initializing database%nstdout:%s%nstderr:%s%n", execResult.getStdout(), execResult.getStderr()));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }


  protected Pool initJDBCPool() {
    return initJDBCPool(new JsonObject());
  }

  protected Pool initJDBCPool(JsonObject extraOption) {
    final JDBCConnectOptions options = new JDBCConnectOptions()
      .setJdbcUrl("jdbc:sqlserver://localhost:" + server.getPort() + ";encrypt=false")
      .setUser(server.getUsername())
      .setPassword(server.getPassword())
      .setExtraConfig(extraOption);
    return JDBCPool.pool(vertx, options, new PoolOptions().setMaxSize(1));
  }

  @Test
  public void simpleTest() throws Exception {
    final Pool client = initJDBCPool();
    // this test would fail if we would attempt to read the generated ids after the end of the cursor
    // the fix implies that we must read them before we close the cursor.
    RowSet<Row> resultSet = client
      .preparedQuery("select * from Fortune")
      .execute()
      .await(20, TimeUnit.SECONDS);
    assertEquals(12, resultSet.size());
  }

  @Test
  public void simpleRSAfterUpdate() throws Exception {
    final Pool client = initJDBCPool();
    client.preparedQuery(//"set nocount on;\n" +
      "INSERT INTO test (field1)\n" + "SELECT ?")
      .executeBatch(new ArrayList<>() {{
      Tuple.of(1);
    }}).await(20, TimeUnit.SECONDS);
  }

  @Test
  public void testProcedures() throws Exception {
    final Pool client = initJDBCPool();

    RowSet<Row> rows = client
      .preparedQuery("{ call rsp_vertx_test_1(?, ?)}")
      .execute(Tuple.of(1, SqlOutParam.OUT(JDBCType.VARCHAR)))
      .await(20, TimeUnit.SECONDS);

    // assert that the first result is received
    assertNotNull(rows);
    assertTrue(rows.size() > 0);
    for (Row row : rows) {
      assertNotNull(row);
    }
    // process the next response
    rows = rows.next();
    assertNotNull(rows);
    assertTrue(rows.property(JDBCPool.OUTPUT));
    assertTrue(rows.size() > 0);
    for (Row row : rows) {
      assertEquals("echo", row.getString(0));
    }
  }

  @Test
  public void testProcedures2() throws Exception {
    final Pool client = initJDBCPool();

    RowSet<Row> rows = client
      .preparedQuery("{ call rsp_vertx_test_2(?)}")
      .execute(Tuple.of(SqlOutParam.OUT(JDBCType.VARCHAR)))
      .await(20, TimeUnit.SECONDS);

    assertNotNull(rows);
    assertTrue(rows.property(JDBCPool.OUTPUT));
    assertTrue(rows.size() > 0);
    for (Row row : rows) {
      assertEquals("echo", row.getString(0));
      assertEquals("echo", row.getString("0"));
    }
  }

  @Test
  public void testConditionalStoredProcedure() throws Exception {
    final Pool client = initJDBCPool();

    RowSet<Row> rows = client
      .preparedQuery("{ call conditional_proc(?)}")
      .execute(Tuple.of(0))
      .await(20, TimeUnit.SECONDS);

    // Should complete without throwing any exception
    assertNotNull(rows);

    rows = client
      .preparedQuery("{ call conditional_proc(?)}")
      .execute(Tuple.of(1))
      .await(20, TimeUnit.SECONDS);

    assertNotNull(rows);
    assertEquals(1, rows.size());
    assertEquals("One", rows.iterator().next().getString(0));
  }

  @Test
  public void testQueryWithJDBCPool() throws Exception {
    final Pool client = initJDBCPool();
    RowSet<Row> rows = client
      .query("SELECT * FROM special_datatype")
      .execute()
      .await(20, TimeUnit.SECONDS);

    assertNotNull(rows);
    assertEquals(1, rows.size());
    assertTrue(rows.columnsNames().containsAll(Arrays.asList("id", "dto")));
    final Row row = rows.iterator().next();
    // by pos
    assertEquals(1, (int)row.getInteger(0));
    assertEquals("2020-12-12 19:30:30.12345 +00:00", row.getString(1));
    // by name
    assertEquals(1, (int)row.getInteger("id"));
    assertEquals("2020-12-12 19:30:30.12345 +00:00", row.getString("dto"));
  }

  @Test
  public void testQueryWithJDBCPoolHasMSSQLDecoder() throws Exception {
    final Pool client = initJDBCPool(new JsonObject().put("decoderCls", MSSQLDecoder.class.getName()));
    RowSet<Row> rows = client
      .query("SELECT * FROM special_datatype")
      .execute()
      .await(20, TimeUnit.SECONDS);
    assertNotNull(rows);
    assertEquals(1, rows.size());
    assertTrue(rows.columnsNames().containsAll(Arrays.asList("id", "dto")));
    final Row row = rows.iterator().next();
    // by pos
    assertEquals(1, (int)row.getInteger(0));
    final OffsetDateTime expected = OffsetDateTime.of(LocalDate.of(2020, 12, 12),
      LocalTime.of(19, 30, 30, 123450000), ZoneOffset.UTC);
    assertEquals(expected, row.getValue(1));
  }
}
