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

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.jdbcclient.impl.FakeDriver;
import io.vertx.jdbcclient.impl.FakeSqlConnectOptions;
import io.vertx.sqlclient.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.Callable;

/**
 * JDBCPool is the interface that allows using the Sql Client API with plain JDBC.
 */
@VertxGen
public interface JDBCPool extends Pool {

  /**
   * The property to be used to retrieve the generated keys
   */
  PropertyKind<Row> GENERATED_KEYS = PropertyKind.create("generated-keys", Row.class);

  /**
   * The property to be used to retrieve the output of the callable statement
   */
  PropertyKind<Boolean> OUTPUT = PropertyKind.create("callable-statement-output", Boolean.class);

  /**
   * Create a JDBC pool which maintains its own data source.
   *
   * @param vertx  the Vert.x instance
   * @param connectOptions the options to configure the connection
   * @param poolOptions the connection pool options
   * @return the client
   */
  static Pool pool(Vertx vertx, JDBCConnectOptions connectOptions, PoolOptions poolOptions) {
    Callable<Connection> connectionCallable = () -> DriverManager.getConnection(connectOptions.getJdbcUrl(), connectOptions.getUser(), connectOptions.getPassword());
    FakeDriver driver = new FakeDriver(connectionCallable);
    return driver.createPool(vertx, () -> Future.succeededFuture(new FakeSqlConnectOptions(connectOptions)), poolOptions, new NetClientOptions(), null);
  }

  /**
   * Create a JDBC pool using a pre-initialized data source, note this data source does not need to be a pool.
   *
   * @param vertx  the Vert.x instance
   * @param dataSource a pre-initialized data source
   * @return the client
   * @since 4.2.0
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  static Pool pool(Vertx vertx, DataSource dataSource, PoolOptions poolOptions) {
    Callable<Connection> connectionCallable = () -> dataSource.getConnection();
    FakeDriver driver = new FakeDriver(connectionCallable);
    return driver.createPool(vertx, () -> Future.succeededFuture(new FakeSqlConnectOptions(new JDBCConnectOptions())), poolOptions, new NetClientOptions(), null);
  }
}
