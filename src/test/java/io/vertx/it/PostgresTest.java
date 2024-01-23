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
import io.vertx.jdbcclient.JDBCConnection;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.jdbcclient.SqlOutParam;
import io.vertx.sqlclient.*;
import io.vertx.test.core.TestUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.postgresql.util.PGInterval;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Arrays;
import java.util.List;

@RunWith(VertxUnitRunner.class)
public class PostgresTest {

  @ClassRule
  public static final RunTestOnContext rule = new RunTestOnContext();

  @SuppressWarnings("rawtypes")
  @ClassRule
  public static final PostgreSQLContainer server = (PostgreSQLContainer) new PostgreSQLContainer("postgres:12-alpine")
    .withInitScript("init-pgsql.sql");

  protected Pool initJDBCPool(JsonObject extraOption) {
    final JDBCConnectOptions options = new JDBCConnectOptions().setJdbcUrl(server.getJdbcUrl())
      .setUser(server.getUsername())
      .setPassword(server.getPassword());
    return JDBCPool.pool(rule.vertx(), options, new PoolOptions().setMaxSize(1));
  }

  @Test
  public void simplePoolCallFunctionTest(TestContext should) {
    final Async test = should.async();
    final Pool pool = initJDBCPool(new JsonObject());

    pool
      .preparedQuery("{ call animal_stats(?, ?, ?) }")
      .execute(Tuple.of(false, SqlOutParam.OUT(JDBCType.BIGINT), SqlOutParam.OUT(JDBCType.REAL)))
      .onFailure(should::fail)
      .onSuccess(rows -> {
        // we can verify if there was an output received from the callable statement
        if (rows.property(JDBCPool.OUTPUT)) {
          // and then iterate the results
          for (Row row : rows) {
            should.assertTrue(row.getValue(0) instanceof Number);
            should.assertEquals(3, row.getInteger(0));
            should.assertTrue(row.getValue(1) instanceof Number);
            should.assertEquals(33.33333206176758, row.getDouble(1));
          }
        }

        test.complete();
      });
  }

  @Test
  public void simplePoolSelectFunctionTest(TestContext should) {
    final Async test = should.async();
    final Pool pool = initJDBCPool(new JsonObject());

    pool
      .preparedQuery("select * from animal_stats(?)")
      .execute(Tuple.of(false))
      .onFailure(should::fail)
      .onSuccess(rows -> {
        // we can verify if there was an output received from the callable statement
        for (Row row : rows) {
          should.assertTrue(row.getValue(0) instanceof Number);
          should.assertEquals(3, row.getInteger(0));
          should.assertTrue(row.getValue(1) instanceof Number);
          should.assertEquals(33.33333206176758, row.getDouble(1));
        }

        test.complete();
      });
  }

  @Test
  public void simpleRowStreamTest(TestContext should) {
    final Async test = should.async();
    final Pool pool = initJDBCPool(new JsonObject());

    List<String> animals = Arrays.asList("dog", "cat", "cow");

    pool
      .getConnection()
      .onFailure(should::fail)
      .onSuccess(connection -> {
        connection
          .prepare("SELECT * FROM ANIMAL")
          .onFailure(should::fail)
          .onSuccess(pq -> {
            // Streams require run within a transaction
            connection.begin()
              .onFailure(should::fail)
              .onSuccess(tx -> {
                // Fetch 1 row at a time
                RowStream<Row> stream = pq.createStream(1);
                // Use the stream
                stream.exceptionHandler(should::fail);
                stream.endHandler(v -> {
                  // Close the stream to release the resources in the database
                  stream.close().onComplete(closed -> tx.commit().onComplete(committed -> test.complete()));
                });
                stream.handler(row -> should.assertTrue(animals.contains(row.getString("name"))));
              });
          });
      });
  }

  @Test
  public void threeParamsTest(TestContext should) {
    final Pool pool = initJDBCPool(new JsonObject());

    boolean expected = TestUtils.randomBoolean();

    testInOut(should, pool, "{ call f_inout_inout_inout(?, ?, ?) }", Tuple.of(
      SqlOutParam.INOUT(true, JDBCType.BOOLEAN),
      SqlOutParam.INOUT(true, JDBCType.BOOLEAN),
      SqlOutParam.INOUT(true, JDBCType.BOOLEAN)), true, true, true);

    testInOut(should, pool, "{ call f_in_inout_inout(?, ?, ?) }", Tuple.of(
      false,
      SqlOutParam.INOUT(false, JDBCType.BOOLEAN),
      SqlOutParam.INOUT(false, JDBCType.BOOLEAN)), true, true);

    testInOut(should, pool, "{ call f_blank_inout_inout(?, ?, ?) }", Tuple.of(
      false,
      SqlOutParam.INOUT(false, JDBCType.BOOLEAN),
      SqlOutParam.INOUT(false, JDBCType.BOOLEAN)), true, true);

    testInOut(should, pool, "{ call f_blank_inout_inout(?, ?, ?) }", Tuple.of(
      false,
      SqlOutParam.INOUT(false, JDBCType.BOOLEAN),
      SqlOutParam.INOUT(false, JDBCType.BOOLEAN)), true, true);

    testInOut(should, pool, "{ call f_in_out_out(?, ?, ?) }", Tuple.of(
      expected,
      SqlOutParam.OUT(JDBCType.BOOLEAN),
      SqlOutParam.OUT(JDBCType.BOOLEAN)), expected, true);

    testInOut(should, pool, "{ call f_blank_out_out(?, ?, ?) }", Tuple.of(
      expected,
      SqlOutParam.OUT(JDBCType.BOOLEAN),
      SqlOutParam.OUT(JDBCType.BOOLEAN)), expected, true );
  }

  private void testInOut(TestContext should, Pool pool, String stmt, Tuple tuple, Object... expected) {
    pool
      .preparedQuery(stmt)
      .execute(tuple).onComplete(should.asyncAssertSuccess(rows -> {
        if (rows.property(JDBCPool.OUTPUT)) {
          Row row = rows.iterator().next();
          should.assertEquals(row.size(), expected.length);
          for (int i = 0;i < row.size();i++) {
            should.assertEquals(expected[i], row.getValue(i));
          }
        }
      }));
  }

  @Test
  public void testPoolQueryTemporalTable(TestContext should) {
    final Async async = should.async();
    Pool pool = initJDBCPool(new JsonObject());
    pool
      .preparedQuery("SELECT * FROM temporal_data_type WHERE id = 1").execute()
      .onSuccess(rows -> {
        should.assertEquals(1, rows.value().size());
        Row row = rows.value().iterator().next();
        should.assertEquals(1, row.getInteger(0));
        should.assertEquals(LocalDate.parse("2022-05-30"), row.getValue(1));
        should.assertEquals(LocalTime.parse("18:00:00"), row.getValue(2));
        should.assertEquals(OffsetTime.parse("04:00:00Z"), row.getValue(3));
        should.assertEquals(LocalDateTime.parse("2022-05-14T07:00:00"), row.getValue(4));
        should.assertEquals(OffsetDateTime.parse("2022-05-14T09:00:00Z"), row.getValue(5));
        should.assertEquals("10 years 3 mons 332 days 20 hours 20 mins 20.999999 secs", row.getValue(6));
        async.complete();
      })
      .onFailure(should::fail);
  }

  @Test
  public void testInsertTemporalTable(TestContext should) throws SQLException {
    final Async async = should.strictAsync(2);
    final Pool pool = initJDBCPool(new JsonObject());
    final Tuple params = Tuple.tuple()
      .addValue(2)
      .addValue(LocalDate.parse("2022-05-30"))
      .addValue(LocalTime.parse("18:00:00"))
      .addValue(OffsetTime.parse("06:00:00+02:00"))
      .addValue(LocalDateTime.parse("2022-05-14T07:00:00"))
      .addValue(OffsetDateTime.parse("2022-05-14T07:00:00-02:00"))
      .addValue(new PGInterval("10 years 3 mons 332 days 20 hours 20 mins 20.999999 secs"));
    pool
      .preparedQuery("INSERT INTO temporal_data_type" +
        "(\"id\", \"Date\", \"Time\", \"TimeTz\", \"Timestamp\", \"TimestampTz\", \"Interval\") " +
        "VALUES (?, ?, ?, ?, ?, ?, ?)")
      .execute(params)
      .onFailure(should::fail)
      .onSuccess(rows -> async.countDown())
      .flatMap(ignore -> pool.preparedQuery("SELECT * FROM temporal_data_type WHERE id = 2").execute())
      .onFailure(should::fail)
      .onSuccess(rows -> {
        final Row row = rows.value().iterator().next();
        should.assertEquals(2, row.getInteger(0));
        should.assertEquals(LocalDate.parse("2022-05-30"), row.getValue(1));
        should.assertEquals(LocalTime.parse("18:00:00"), row.getValue(2));
        should.assertEquals(OffsetTime.parse("04:00:00Z"), row.getValue(3));
        should.assertEquals(LocalDateTime.parse("2022-05-14T07:00:00"), row.getValue(4));
        should.assertEquals(OffsetDateTime.parse("2022-05-14T09:00:00Z"), row.getValue(5));
        should.assertEquals("10 years 3 mons 332 days 20 hours 20 mins 20.999999 secs", row.getValue(6));
        async.complete();
      });
  }

  @Test
  public void queryTimeoutTest(TestContext should) {
    final Pool pool = initJDBCPool(new JsonObject());
    pool.withConnection(conn -> {
      ((JDBCConnection)conn).setQueryTimeout(1);
      return conn.query("select pg_sleep(2)").execute();
    }).onComplete(should.asyncAssertFailure(err1 -> {
      should.assertTrue(err1.getMessage().contains("canceling statement due to user request"));
      pool.withConnection(conn -> conn.query("select pg_sleep(2)").execute()).onComplete(should.asyncAssertSuccess());
    }));
  }
}
