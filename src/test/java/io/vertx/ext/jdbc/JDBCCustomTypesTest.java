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

package io.vertx.ext.jdbc;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.test.core.VertxTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author <a href="mailto:pmlopes@gmail.com">Paulo Lopes</a>
 */
public class JDBCCustomTypesTest extends VertxTestBase {

  private static final List<String> SQL = new ArrayList<>();

  static {
    SQL.add("drop table if exists t");
    SQL.add("create table t (u UUID, d DATE, t TIME, ts TIMESTAMP)");
    SQL.add("insert into t (u) values (random_uuid())");
  }

  private JsonObject config;
  private SQLClient client;

  @Before
  public void setUp() throws Exception {
    config = ConfigFactory.createConfigForH2();
    try (Connection conn = DriverManager.getConnection(config.getString("url"))) {
      for (String sql : SQL) {
        try (Statement statement = conn.createStatement()) {
          statement.execute(sql);
        }
      }
    }
    super.setUp();
    client = JDBCClient.createNonShared(vertx, config);
  }

  @After
  public void after() throws Exception {
    client.close();
    try (Connection conn = DriverManager.getConnection(config.getString("url"))) {
      try (Statement statement = conn.createStatement()) {
        statement.execute("SHUTDOWN");
      }
    }
    super.after();
  }

  @Test
  public void testCustom() {
    String sql = "SELECT u FROM t";
    connection().query(sql, onSuccess(resultSet -> {
      assertNotNull(resultSet);
      assertEquals(1, resultSet.getResults().size());
      // we expect a String since UUID will be converted with the fallback mode
      assertNotNull(resultSet.getResults().get(0).getString(0));
      testComplete();
    }));

    await();
  }

  @Test
  public void testCustomInsert() {
    String sql = "INSERT INTO t (u, t, d, ts) VALUES (?, ?, ?, ?)";
    final String uuid = UUID.randomUUID().toString();

    final SQLConnection conn = connection();

    conn.setAutoCommit(false, tx -> {
      if (tx.succeeded()) {
        conn.updateWithParams(sql, new JsonArray().add(uuid).add("09:00:00").add("2015-03-16").add(Instant.now()), onSuccess(resultSet -> {
          testComplete();
        }));
      }
    });

    await();
  }

  private SQLConnection connection() {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<SQLConnection> ref = new AtomicReference<>();
    client.getConnection(onSuccess(conn -> {
      ref.set(conn);
      latch.countDown();
    }));

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return ref.get();
  }
}
