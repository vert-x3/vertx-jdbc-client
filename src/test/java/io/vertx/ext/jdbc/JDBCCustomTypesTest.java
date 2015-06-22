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

import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.test.core.VertxTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author <a href="mailto:pmlopes@gmail.com">Paulo Lopes</a>
 */
public class JDBCCustomTypesTest extends VertxTestBase {

  protected JDBCClient client;

  private static final List<String> SQL = new ArrayList<>();

  static {
    SQL.add("drop table if exists t");
    SQL.add("create table t (u UUID)");
    SQL.add("insert into t (u) values (random_uuid())");
  }

  @BeforeClass
  public static void createDb() throws Exception {
    Connection conn = DriverManager.getConnection(config().getString("url"));
    for (String sql : SQL) {
      conn.createStatement().execute(sql);
    }
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    client = JDBCClient.createNonShared(vertx, config());
  }

  @After
  public void after() throws Exception {
    client.close();
    super.after();
  }

  protected static JsonObject config() {
    return new JsonObject()
        .put("url", "jdbc:h2:mem:test?shutdown=true")
        .put("driver_class", "org.h2.Driver");
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
