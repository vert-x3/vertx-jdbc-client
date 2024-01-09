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
import io.vertx.core.Vertx;
import io.vertx.core.impl.CloseFuture;
import io.vertx.jdbcclient.impl.JDBCPoolImpl;
import io.vertx.sqlclient.*;

import javax.sql.DataSource;
import java.sql.DriverManager;

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
  static JDBCPool pool(Vertx vertx, JDBCConnectOptions connectOptions, PoolOptions poolOptions) {
    CloseFuture closeFuture = new CloseFuture();
    JDBCPoolImpl pool = new JDBCPoolImpl(
      vertx,
      connectOptions,
      () -> DriverManager.getConnection(connectOptions.getJdbcUrl(), connectOptions.getUser(), connectOptions.getPassword()),
      poolOptions,
      connectOptions.getUser(),
      connectOptions.getDatabase(),
      closeFuture);
    pool.init();
    return pool;
  }

  /**
   * Create a JDBC pool using a pre-initialized data source.
   *
   * @param vertx  the Vert.x instance
   * @param dataSource a pre-initialized data source
   * @return the client
   * @since 4.2.0
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  static JDBCPool pool(Vertx vertx, DataSource dataSource) {
    throw new UnsupportedOperationException("Not yet implemented (but could be)");
  }
}
