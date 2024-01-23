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

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.jdbcclient.SqlOutParam;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.JDBCType;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;

@RunWith(VertxUnitRunner.class)
public class MSSQLTest {

  @ClassRule
  public static final RunTestOnContext rule = new RunTestOnContext();

  private static MSSQLServer server;

  @BeforeClass
  public static void setup(TestContext should) {
    final Async test = should.async();
    rule.vertx().executeBlocking(() -> {
      server = new MSSQLTest.MSSQLServer();
      server.withInitScript("init-mssql.sql");
      server.start();
      return null;
    }, true).onSuccess(o -> test.complete()).onFailure(should::fail);
  }

  @AfterClass
  public static void tearDown() {
    server.close();
  }

  static class MSSQLServer extends MSSQLServerContainer {

    @Override
    protected void configure() {
      this.addExposedPort(MSSQLServerContainer.MS_SQL_SERVER_PORT);
      this.addEnv("ACCEPT_EULA", "Y");
      this.addEnv("SA_PASSWORD", this.getPassword());
    }

  }

  protected Pool initJDBCPool() {
    return initJDBCPool(new JsonObject());
  }

  protected Pool initJDBCPool(JsonObject extraOption) {
    final JDBCConnectOptions options = new JDBCConnectOptions().setJdbcUrl(server.getJdbcUrl())
      .setUser(server.getUsername())
      .setPassword(server.getPassword())
      .setExtraConfig(extraOption);
    return JDBCPool.pool(rule.vertx(), options, new PoolOptions().setMaxSize(1));
  }

  @Test
  public void simpleTest(TestContext should) {
    final Async test = should.async();
    final Pool client = initJDBCPool();
    // this test would fail if we would attempt to read the generated ids after the end of the cursor
    // the fix implies that we must read them before we close the cursor.
    client.preparedQuery("select * from Fortune").execute().onComplete(should.asyncAssertSuccess(resultSet -> {
      should.assertEquals(12, resultSet.size());
      test.complete();
    }));
  }

  @Test
  public void simpleRSAfterUpdate(TestContext should) {
    final Async test = should.async();
    final Pool client = initJDBCPool();
    client.preparedQuery(//"set nocount on;\n" +
      "INSERT INTO test (field1)\n" + "SELECT ?").executeBatch(new ArrayList<Tuple>() {{
      Tuple.of(1);
    }}).onFailure(should::fail).onSuccess(rowSet -> test.complete());
  }

  @Test
  public void testProcedures(TestContext should) {
    final Async test = should.async();
    final Pool client = initJDBCPool();

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
    final Pool client = initJDBCPool();

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
    final Pool client = initJDBCPool();
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
    final Pool client = initJDBCPool(new JsonObject().put("decoderCls", MSSQLDecoder.class.getName()));
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
}
