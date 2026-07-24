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

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.clickhouse.ClickHouseContainer;

@RunWith(VertxUnitRunner.class)
public class ClickHouseTest {

  private Vertx vertx;
  private ClickHouseContainer container;
  protected Pool client;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    container = new ClickHouseContainer("clickhouse/clickhouse-server:21.11-alpine");
    container.withInitScript("init-clickhouse.sql");
    container.start();
    JDBCConnectOptions connectOptions = new JDBCConnectOptions()
      .setJdbcUrl(container.getJdbcUrl())
      .setUser(container.getUsername())
      .setPassword(container.getPassword());
    client = JDBCPool.pool(vertx, connectOptions, new PoolOptions());
  }

  @After
  public void after(TestContext should) {
    Async cleanup = should.async();
    client.close().onComplete(should.asyncAssertSuccess(res1 -> {
      vertx.close().onComplete(should.asyncAssertSuccess(res2 -> {
        container.close();
        cleanup.complete();
      }));
    }));
  }

  @Test
  public void simpleTest(TestContext should) {
    Async test = should.async();
    client.query("select * from arr_test")
      .execute().onComplete(should.asyncAssertSuccess(res -> {
        should.assertEquals(1, res.size());
        Row row = res.iterator().next();
        should.assertEquals(new JsonObject()
          .put("id", "1ff954bb-9808-4309-9955-fccf1a26266e")
          .put("value", new JsonArray().add(0.0d).add(1.0d)), row.toJson());
        test.complete();
      }));
  }
}
