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

import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MySQLContainer;

@RunWith(VertxUnitRunner.class)
public class MySQLSavepointTest extends SavepointTestBase {

  @ClassRule
  public static final RunTestOnContext rule = new RunTestOnContext();

  @ClassRule
  public static final MySQLContainer<?> server = new MySQLContainer<>("mysql:8.0")
    .withInitScript("init-mysql.sql");

  @Override
  protected Pool pool() {
    JDBCConnectOptions options = new JDBCConnectOptions()
      .setJdbcUrl(server.getJdbcUrl())
      .setUser(server.getUsername())
      .setPassword(server.getPassword());
    return JDBCPool.pool(rule.vertx(), options, new PoolOptions().setMaxSize(1));
  }

  @Override
  protected Pool checkerPool() {
    JDBCConnectOptions options = new JDBCConnectOptions()
      .setJdbcUrl(server.getJdbcUrl())
      .setUser(server.getUsername())
      .setPassword(server.getPassword());
    return JDBCPool.pool(rule.vertx(), options, new PoolOptions().setMaxSize(1));
  }
}
