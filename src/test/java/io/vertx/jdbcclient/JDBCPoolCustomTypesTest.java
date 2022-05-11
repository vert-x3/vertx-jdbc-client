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

package io.vertx.jdbcclient;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.junit.*;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class JDBCPoolCustomTypesTest extends ClientTestBase {

  private static final JDBCConnectOptions options = new JDBCConnectOptions()
    .setJdbcUrl("jdbc:h2:mem:test-" + JDBCPoolCustomTypesTest.class.getSimpleName() + ";DB_CLOSE_DELAY=-1");

  private static final List<String> SQL = new ArrayList<>();

  static {
    SQL.add("drop table if exists t");
    SQL.add("create table t (u UUID, d DATE, t TIME, ts TIMESTAMP)");
    SQL.add("insert into t (u) values (random_uuid())");
  }

  public static void resetDb() throws Exception {
    Connection conn = DriverManager.getConnection(options.getJdbcUrl());
    for (String sql : SQL) {
      conn.createStatement().execute(sql);
    }
  }

  @Before
  public void setUp() throws Exception {
    resetDb();
    super.setUp();
  }

  @Override
  protected JDBCConnectOptions connectOptions() {
    return options;
  }

  @Test
  public void testSelectUUID(TestContext should) {
    final Async test = should.async();

    String sql = "SELECT u FROM t";

    client
      .query(sql)
      .execute()
      .onFailure(should::fail)
      .onSuccess(rows -> {
        should.assertNotNull(rows);
        should.assertEquals(1, rows.size());
        for (Row row : rows) {
          should.assertNotNull(row.getUUID(0));
          UUID uuid = row.getUUID(0);
        }
        test.complete();
      });
  }

  @Test
  public void testCustomInsert(TestContext should) {
    final Async test = should.async();

    String sql = "INSERT INTO t (u, t, d, ts) VALUES (?, ?, ?, ?)";

    client
      .preparedQuery(sql)
      .execute(Tuple.of(UUID.randomUUID(), LocalTime.of(9, 0, 0), LocalDate.of(2020, Month.JUNE, 19), LocalDateTime.now()))
      .onFailure(should::fail)
      .onSuccess(rows -> {
        should.assertEquals(1, rows.rowCount());

        // load and see
        client
          .query("SELECT u, t, d, ts from t where ts is not null")
          .execute()
          .onFailure(should::fail)
          .onSuccess(rows2 -> {
            should.assertEquals(1, rows2.size());
            for (Row row : rows2) {
              should.assertNotNull(row.getUUID("U"));
              should.assertNotNull(row.getLocalTime("T"));
              should.assertNotNull(row.getLocalDate("D"));
              should.assertNotNull(row.getLocalDateTime("TS"));
            }
            test.complete();
          });
      });
  }
}
