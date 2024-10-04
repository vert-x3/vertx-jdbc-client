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

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.jdbc.spi.DataSourceProvider;
import io.vertx.ext.jdbc.spi.impl.AgroalCPDataSourceProvider;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.jdbcclient.SqlOutParam;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
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

import static org.testcontainers.containers.BindMode.READ_ONLY;

@RunWith(VertxUnitRunner.class)
public class MSSQLTest {

  @ClassRule
  public static final RunTestOnContext rule = new RunTestOnContext();

  private static MSSQLServer server;

  @BeforeClass
  public static void setup(TestContext should) {
    final Async test = should.async();
    rule.vertx().executeBlocking(() -> {
      server = new MSSQLServer();
      server.start();
      return null;
    }, true).onSuccess(o -> test.complete()).onFailure(should::fail);
  }

  @AfterClass
  public static void tearDown() {
    server.stop();
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


  protected JDBCPool initJDBCPool() {
    return initJDBCPool(new JsonObject());
  }

  protected JDBCPool initJDBCPool(JsonObject extraOption) {
    final JDBCConnectOptions options = new JDBCConnectOptions()
      .setJdbcUrl("jdbc:sqlserver://localhost:" + server.getPort() + ";encrypt=false")
      .setUser(server.getUsername())
      .setPassword(server.getPassword());
    final DataSourceProvider provider = new AgroalCPDataSourceProvider();
    provider.init(provider.toJson(options, new PoolOptions().setMaxSize(1)).mergeIn(extraOption));
    return JDBCPool.pool(rule.vertx(), provider);
  }

  protected JDBCClient initJDBCClient(JsonObject extraOption) {
    JsonObject options = new JsonObject().put("url", "jdbc:sqlserver://localhost:" + server.getPort() + ";encrypt=false")
      .put("user", server.getUsername())
      .put("password", server.getPassword());
    return JDBCClient.createShared(rule.vertx(), options.mergeIn(extraOption, true), "dbName");
  }

  @Test
  public void simpleTest(TestContext should) {
    final Async test = should.async();
    final JDBCPool client = initJDBCPool();
    // this test would fail if we would attempt to read the generated ids after the end of the cursor
    // the fix implies that we must read them before we close the cursor.
    client.preparedQuery("select * from Fortune")
      .execute().onComplete(should.asyncAssertSuccess(resultSet -> {
      should.assertEquals(12, resultSet.size());
      test.complete();
    }));
  }

  @Test
  public void simpleRSAfterUpdate(TestContext should) {
    final Async test = should.async();
    final JDBCPool client = initJDBCPool();
    client.preparedQuery(//"set nocount on;\n" +
      "INSERT INTO test (field1)\n" + "SELECT ?").executeBatch(new ArrayList<Tuple>() {{
      Tuple.of(1);
    }}).onFailure(should::fail).onSuccess(rowSet -> test.complete());
  }

  @Test
  public void testProcedures(TestContext should) {
    final Async test = should.async();
    final JDBCPool client = initJDBCPool();

    client.preparedQuery("{ call rsp_vertx_test_1(?, ?)}")
      .execute(Tuple.of(1, SqlOutParam.OUT(JDBCType.VARCHAR)))
      .onFailure(should::fail)
      .onSuccess(rows -> {
        // assert that the first result is received
        should.assertNotNull(rows);
        should.assertTrue(rows.size() > 0);
        for (Row row : rows) {
          should.assertNotNull(row);
        }
        // process the next response
        rows = rows.next();
        should.assertNotNull(rows);
        should.assertTrue(rows.property(JDBCPool.OUTPUT));
        should.assertTrue(rows.size() > 0);
        for (Row row : rows) {
          should.assertEquals("echo", row.getString(0));
        }
        test.complete();
      });
  }

  @Test
  public void testProcedures2(TestContext should) {
    final Async test = should.async();
    final JDBCPool client = initJDBCPool();

    client.preparedQuery("{ call rsp_vertx_test_2(?)}")
      .execute(Tuple.of(SqlOutParam.OUT(JDBCType.VARCHAR)))
      .onFailure(should::fail)
      .onSuccess(rows -> {
        should.assertNotNull(rows);
        should.assertTrue(rows.property(JDBCPool.OUTPUT));
        should.assertTrue(rows.size() > 0);
        for (Row row : rows) {
          should.assertEquals("echo", row.getString(0));
          should.assertEquals("echo", row.getString("0"));
        }
        test.complete();
      });
  }

  @Test
  public void testQueryWithJDBCPool(TestContext should) {
    final Async async = should.async();
    final JDBCPool client = initJDBCPool();
    client.query("SELECT * FROM special_datatype").execute().onFailure(should::fail).onSuccess(rows -> {
      should.assertNotNull(rows);
      should.assertEquals(1, rows.size());
      should.assertTrue(rows.columnsNames().containsAll(Arrays.asList("id", "dto")));
      final Row row = rows.iterator().next();
      // by pos
      should.assertEquals(1, row.getInteger(0));
      should.assertEquals("2020-12-12 19:30:30.12345 +00:00", row.getString(1));
      // by name
      should.assertEquals(1, row.getInteger("id"));
      should.assertEquals("2020-12-12 19:30:30.12345 +00:00", row.getString("dto"));
      async.complete();
    });
  }

  @Test
  public void testQueryWithJDBCPoolHasMSSQLDecoder(TestContext should) {
    final Async async = should.async();
    final JDBCPool client = initJDBCPool(new JsonObject().put("decoderCls", MSSQLDecoder.class.getName()));
    client.query("SELECT * FROM special_datatype").execute().onFailure(should::fail).onSuccess(rows -> {
      should.assertNotNull(rows);
      should.assertEquals(1, rows.size());
      should.assertTrue(rows.columnsNames().containsAll(Arrays.asList("id", "dto")));
      final Row row = rows.iterator().next();
      // by pos
      should.assertEquals(1, row.getInteger(0));
      final OffsetDateTime expected = OffsetDateTime.of(LocalDate.of(2020, 12, 12),
        LocalTime.of(19, 30, 30, 123450000), ZoneOffset.UTC);
      should.assertEquals(expected, row.getValue(1));
      async.complete();
    });
  }

  @Test
  public void testQueryWithJDBCClientHasMSSQLDecoder(TestContext should) {
    final Async async = should.async();
    final JDBCClient client = initJDBCClient(new JsonObject().put("decoderCls", MSSQLDecoder.class.getName()));
    client.query("SELECT * FROM special_datatype", should.asyncAssertSuccess(resultSet -> {
      Assert.assertEquals(1, resultSet.getNumRows());
      final JsonArray row = resultSet.getResults().get(0);
      Assert.assertEquals(1, (int) row.getInteger(0));
      final Object dto = row.getValue(1);
      final OffsetDateTime expected = OffsetDateTime.of(LocalDate.of(2020, 12, 12),
        LocalTime.of(19, 30, 30, 123450000), ZoneOffset.UTC);
      Assert.assertEquals(OffsetDateTime.class, dto.getClass());
      Assert.assertEquals(expected, dto);
      async.complete();
    }));
  }

}
