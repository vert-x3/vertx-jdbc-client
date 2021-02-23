/*
 * Copyright (c) 2011-2021 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.ext.jdbc;

import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.ext.sql.SQLClient;
import io.vertx.test.core.VertxTestBase;
import io.vertx.test.fakemetrics.FakeMetricsFactory;
import io.vertx.test.fakemetrics.FakePoolMetrics;
import org.h2.tools.Server;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class PoolTest extends VertxTestBase {

  Server server;
  SQLClient client;

  @Override
  public void setUp() throws Exception {
    server = Server.createTcpServer("-tcp").start();
    super.setUp();
  }

  @Override
  protected VertxOptions getOptions() {
    MetricsOptions options = new MetricsOptions().setEnabled(true);
    options.setFactory(new FakeMetricsFactory());
    return new VertxOptions().setMetricsOptions(options);
  }

  @Test(timeout = 5000)
  public void testUseAvailableResources() {
    int poolSize = 3;
    waitFor(poolSize + 1);

    JsonObject config = new JsonObject()
      .put("url", "jdbc:h2:tcp://localhost/mem:test")
      .put("driver_class", "org.h2.Driver")
      .put("initial_pool_size", poolSize)
      .put("max_pool_size", poolSize);
    client = JDBCClient.createShared(vertx, config);

    vertx.setPeriodic(10, timerId -> {
      FakePoolMetrics metrics = getMetrics();
      if (metrics != null && poolSize == metrics.numberOfRunningTasks()) {
        vertx.cancelTimer(timerId);
        complete();
      }
    });

    client.query("CREATE ALIAS SLEEP FOR \"io.vertx.ext.jdbc.PoolTest.sleep\";", onSuccess(def -> {
      for (int i = 0; i < poolSize; i++) {
        client.query("SELECT SLEEP(500)", onSuccess(rs -> complete()));
      }
    }));

    await();
  }

  @SuppressWarnings("unused")
  public static int sleep(int howLong) {
    try {
      Thread.sleep(howLong);
    } catch (InterruptedException e) {
      return -1;
    }
    return howLong;
  }

  @After
  public void after() throws Exception {
    if (client != null) {
      CountDownLatch latch = new CountDownLatch(1);
      client.close(ar -> latch.countDown());
      awaitLatch(latch);
    }
    super.after();
    if (server != null) {
      server.stop();
    }
  }

  private FakePoolMetrics getMetrics() {
    return (FakePoolMetrics) FakePoolMetrics.getPoolMetrics().get(JDBCClient.DEFAULT_DS_NAME);
  }
}
