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

package io.vertx.jdbcclient;

import io.vertx.core.VertxOptions;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.core.spi.VertxMetricsFactory;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.test.core.VertxTestBase;
import io.vertx.test.fakemetrics.FakeMetricsFactory;
import org.h2.tools.Server;
import org.junit.After;
import org.junit.Test;

public class PoolTest extends VertxTestBase {

  Server server;
  Pool client;

  @Override
  public void setUp() throws Exception {
    server = Server.createTcpServer("-tcp").start();
    super.setUp();
  }

  @Override
  protected VertxOptions getOptions() {
    MetricsOptions options = new MetricsOptions().setEnabled(true);
    return new VertxOptions().setMetricsOptions(options);
  }

  @Override
  protected VertxMetricsFactory getMetrics() {
    return new FakeMetricsFactory();
  }

  @Test(timeout = 30000)
  public void testUseAvailableResources() {
    int poolSize = 3;
    waitFor(poolSize + 1 - 1);

    client = JDBCPool.pool(vertx, new JDBCConnectOptions().setJdbcUrl("jdbc:h2:mem:test_mem"), new PoolOptions().setMaxSize(poolSize));

    // Pool metrics are not yet implemented
/*
    vertx.setPeriodic(10, timerId -> {
      FakePoolMetrics metrics = fakeMetrics();
      System.out.println(metrics.numberOfRunningTasks());
      if (metrics != null && poolSize == metrics.numberOfRunningTasks()) {
        vertx.cancelTimer(timerId);
        complete();
      }
    });
*/

    client.query("CREATE ALIAS SLEEP FOR \"" + PoolTest.class.getName() + ".sleep\";").execute().onComplete(onSuccess(def -> {
      for (int i = 0; i < poolSize; i++) {
        client.query("SELECT SLEEP(500)").execute().onComplete(onSuccess(rs -> {
          complete();
        }));
      }
    }));

    await();
  }

  @SuppressWarnings("unused")
  public static int sleep(int howLong) {
    try {
      Thread.sleep(howLong);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return -1;
    }
    return howLong;
  }

  @After
  public void after() throws Exception {
    if (client != null) {
      client.close()
              .toCompletionStage()
              .toCompletableFuture()
              .get();
    }
    super.after();
    if (server != null) {
      server.stop();
    }
  }

//  private FakePoolMetrics fakeMetrics() {
//    return (FakePoolMetrics) FakePoolMetrics.getPoolMetrics().get(JDBCClient.DEFAULT_DS_NAME);
//  }
}
