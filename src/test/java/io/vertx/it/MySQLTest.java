/*
 * Copyright (c) 2011-2026 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.it;

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
}
