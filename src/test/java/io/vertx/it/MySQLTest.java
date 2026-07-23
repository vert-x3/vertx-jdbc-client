/*
 * Copyright (c) 2011-2026 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *      The Eclipse Public License is available at
 *      http://www.eclipse.org/legal/epl-v10.html
 *
 *      The Apache License v2.0 is available at
 *      http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.it;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MySQLContainer;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

@RunWith(VertxUnitRunner.class)
public class MySQLTest {

  @ClassRule
  public static final RunTestOnContext rule = new RunTestOnContext();

  @ClassRule
  public static final MySQLContainer<?> server = new MySQLContainer<>("mysql:8.0")
    .withInitScript("init-mysql.sql");

  protected Pool initJDBCPool(JsonObject extraOption) {
    final JDBCConnectOptions options = new JDBCConnectOptions().setJdbcUrl(server.getJdbcUrl())
      .setUser(server.getUsername())
      .setPassword(server.getPassword());
    return JDBCPool.pool(rule.vertx(), options, new PoolOptions().setMaxSize(1));
  }

  @Test
  public void testReadBinaryData(TestContext should) {
    Pool pool = initJDBCPool(new JsonObject());
    pool
      .query("SELECT binary_col FROM binary_data_type WHERE id = 1").execute()
      .onComplete(should.asyncAssertSuccess(rows -> {
        should.assertEquals(1, rows.size());
        Row row = rows.value().iterator().next();
        byte[] expected = new byte[]{
          0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF,
          0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF
        };
        Buffer actual = row.getBuffer(0);
        should.assertNotNull(actual);
        should.verify(v -> assertArrayEquals(expected, actual.getBytes()));
      }));
  }

  @Test
  public void testInsertBinaryData(TestContext should) {
    Pool pool = initJDBCPool(new JsonObject());
    byte[] expected = new byte[]{
      (byte) 0xFE, (byte) 0xDC, (byte) 0xBA, (byte) 0x98, 0x76, 0x54, 0x32, 0x10,
      (byte) 0xFE, (byte) 0xDC, (byte) 0xBA, (byte) 0x98, 0x76, 0x54, 0x32, 0x10
    };
    Buffer buffer = Buffer.buffer(expected);
    pool
      .preparedQuery("INSERT INTO binary_data_type (id, binary_col) VALUES (?, ?)").execute(Tuple.of(2, buffer))
      .onComplete(should.asyncAssertSuccess(result -> {
        should.assertEquals(1, result.rowCount());
        pool
          .query("SELECT binary_col FROM binary_data_type WHERE id = 2").execute()
          .onComplete(should.asyncAssertSuccess(rows -> {
            should.assertEquals(1, rows.size());
            Row row = rows.value().iterator().next();
            Buffer actual = row.getBuffer(0);
            should.assertNotNull(actual);
            should.verify(v -> assertArrayEquals(expected, actual.getBytes()));
          }));
      }));
  }

  @Test
  public void testGeneratedKeys(TestContext should) {
    Pool pool = initJDBCPool(new JsonObject());
    pool.query("select max(ID) from animal")
      .execute()
      .onComplete(should.asyncAssertSuccess(maxIdResult -> {
        int maxId = maxIdResult.value().iterator().next().getInteger(0); // we already have 3 animals
        pool.preparedQuery("insert into animal (is_pet, name) values (?, ?)")
          .executeBatch(Arrays.asList(Tuple.of(true, "pig"), Tuple.of(false, "bear")))
          .onComplete(should.asyncAssertSuccess(inserted -> {
            should.assertNotNull(inserted);
            should.assertEquals(2, inserted.rowCount());
            Row insertedRow = inserted.property(JDBCPool.GENERATED_KEYS);
            List<Row> insertedRows = inserted.property(JDBCPool.GENERATED_KEYS_LIST).rows();
            should.assertEquals(2, insertedRows.size());
            should.assertTrue(insertedRows.contains(insertedRow));
            for (Row row : insertedRows) {
              should.assertTrue(row.getInteger(0) > maxId);
            }
            pool.preparedQuery("delete from animal where ID > ?")
              .execute(Tuple.of(maxId))
              .onComplete(should.asyncAssertSuccess(deleted -> {
                should.assertNotNull(deleted);
                should.assertEquals(2, deleted.rowCount());
                // We cannot check for deleted records
                // because MySQL does not support this feature for the DELETE statement
              }));
          }));
      }));
  }

  @Test
  public void testAutoCommitRestoredAfterCommit(TestContext should) {
    testAutoCommitRestored(should, true);
  }

  @Test
  public void testAutoCommitRestoredAfterRollback(TestContext should) {
    testAutoCommitRestored(should, false);
  }

  private void testAutoCommitRestored(TestContext should, boolean commit) {
    Pool pool = initJDBCPool(new JsonObject());
    Pool checkerPool = initJDBCPool(new JsonObject());
    pool.query("TRUNCATE TABLE people").execute().onComplete(should.asyncAssertSuccess(v0 -> {
      pool.withTransaction(conn -> {
        if (!commit) {
          return Future.failedFuture("boom");
        }
        return conn.preparedQuery("INSERT INTO people (name) VALUES (?)")
          .execute(Tuple.of("Thomas"));
      }).otherwiseEmpty().onComplete(should.asyncAssertSuccess(v1 -> {
        pool.withConnection(conn -> {
          return conn.preparedQuery("INSERT INTO people (name) VALUES (?)")
            .execute(Tuple.of("Julien"));
        }).compose(v2 -> {
          return checkerPool.withConnection(conn -> {
            return conn.query("SELECT COUNT(*) FROM people").execute();
          });
        }).onComplete(should.asyncAssertSuccess(rows -> {
          should.assertEquals(1, rows.size());
          should.assertEquals(commit ? 2 : 1, rows.iterator().next().getInteger(0));
        }));
      }));
    }));
  }
}
