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

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.jdbc.spi.DataSourceProvider;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.jdbcclient.SqlOutParam;
import io.vertx.jdbcclient.impl.AgroalCPDataSourceProvider;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.Tuple;
import org.junit.*;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;

@RunWith(VertxUnitRunner.class)
public class PostgresTest {

  @ClassRule
  public static final RunTestOnContext rule = new RunTestOnContext();

  private static PostgreSQLContainer server;

  @BeforeClass
  public static void setup(TestContext should) {
    final Async test = should.async();
    rule.vertx().executeBlocking(p -> {
      try {
        server = new PostgreSQLContainer("postgres:12");
        server.withInitScript("init-pgsql.sql");
        server.start();
        p.complete();
      } catch (RuntimeException e) {
        p.fail(e);
      }
    }, true).onSuccess(o -> test.complete()).onFailure(should::fail);
  }

  @AfterClass
  public static void tearDown() {
    server.close();
  }

  protected JDBCPool initJDBCPool(JsonObject extraOption) {
    final JDBCConnectOptions options = new JDBCConnectOptions().setJdbcUrl(server.getJdbcUrl())
      .setUser(server.getUsername())
      .setPassword(server.getPassword());
    final DataSourceProvider provider = new AgroalCPDataSourceProvider(options, new PoolOptions().setMaxSize(1));
    return JDBCPool.pool(rule.vertx(), provider.init(extraOption));
  }

  protected JDBCClient initJDBCClient(JsonObject extraOption) {
    JsonObject options = new JsonObject().put("url", server.getJdbcUrl())
      .put("user", server.getUsername())
      .put("password", server.getPassword());
    return JDBCClient.createShared(rule.vertx(), options.mergeIn(extraOption, true), "dbName");
  }

  @Test
  public void simpleClientTest(TestContext should) {
    final Async test = should.async();
    final JDBCClient client = initJDBCClient(new JsonObject());

    client.callWithParams(
      "{ call animal_stats(?, ?, ?) }",
      new JsonArray().add(false),
      new JsonArray().addNull().add("BIGINT").add("REAL"),
      asyncResult -> {
        if (asyncResult.failed()) {
          should.fail(asyncResult.cause());
        } else {
          ResultSet statsResult = asyncResult.result();
          JsonArray output = statsResult.getOutput();
          System.out.println(new JsonObject().put("stats", output.toString()).encodePrettily());
          test.complete();
        }
      }
    );
  }

  @Test
  public void simplePoolTest(TestContext should) {
    final Async test = should.async();
    final JDBCPool pool = initJDBCPool(new JsonObject());

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
  public void simpleRowStreamTest(TestContext should) {
    final Async test = should.async();
    final JDBCPool pool = initJDBCPool(new JsonObject());

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
                  stream.close(closed -> tx.commit(committed -> test.complete()));
                });
                stream.handler(row -> should.assertTrue(animals.contains(row.getString("name"))));
              });
          });
      });
  }

  @Test
  public void threeParamsTest(TestContext should) {
    final Async test = should.async();

    final JDBCClient client = initJDBCClient(new JsonObject());
    // 3x INOUT
    callWithInOut(client, "{ call f_inout_inout_inout(?, ?, ?) }",
      new JsonArray().add(false).add(false).add(false),             // OK
      new JsonArray().add("BOOLEAN").add("BOOLEAN").add("BOOLEAN"))
      .compose(v ->
        // 1x IN, 2x INOUT
        callWithInOut(client, "{ call f_in_inout_inout(?, ?, ?) }",
          new JsonArray().add(false).add(false).add(false),             // OK
          new JsonArray().addNull().add("BOOLEAN").add("BOOLEAN")))
      .compose(v ->
        // 1x implicit IN, 2x INOUT
        callWithInOut(client, "{ call f_blank_inout_inout(?, ?, ?) }",
          new JsonArray().add(false).add(false).add(false),             // OK
          new JsonArray().addNull().add("BOOLEAN").add("BOOLEAN")))
      .compose(v ->
        // 1x IN, 2x OUT
        callWithInOut(client, "{ call f_in_out_out(?, ?, ?) }",
          new JsonArray().add(false),                                   // OK
          new JsonArray().addNull().add("BOOLEAN").add("BOOLEAN")))
      .compose(v ->
        // 1x implicit IN, 2x OUT
        callWithInOut(client, "{ call f_blank_out_out(?, ?, ?) }",
          new JsonArray(),                                   // failing
          new JsonArray().addNull().add("BOOLEAN").add("BOOLEAN")))
      .onFailure(should::fail)
      .onSuccess(v -> test.complete());
  }

  private Future<Void> callWithInOut(JDBCClient client, String sql, JsonArray in, JsonArray out) {
    final Promise<Void> promise = ((VertxInternal) rule.vertx()).promise();

    client.callWithParams(
      sql,
      in,
      out,
      asyncResult -> {
        if (asyncResult.failed()) {
          printResults("FAIL ", sql, in, out, asyncResult.cause().getMessage());
          promise.fail(asyncResult.cause());
        } else {
          ResultSet statsResult = asyncResult.result();
          JsonArray output = statsResult.getOutput();
          printResults("OK   ", sql, in, out, output.encodePrettily());
          promise.complete();
        }
      });

    return promise.future();
  }

  public void printResults(String succMsg, String sql, JsonArray in, JsonArray out, String result) {
    System.out.println(succMsg + " sql: " + sql + " \n"
      + " IN: " + in.toString() + "\n"
      + " OUT: " + out.toString() + "\n"
      + " result: " + result);
  }
}
