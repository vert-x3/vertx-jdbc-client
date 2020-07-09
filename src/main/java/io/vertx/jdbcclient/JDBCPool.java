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

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.impl.JDBCClientImpl;
import io.vertx.jdbcclient.impl.AgroalCPDataSourceProvider;
import io.vertx.jdbcclient.impl.JDBCPoolImpl;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.impl.tracing.QueryTracer;

import java.util.UUID;

/**
 * JDBCPool is the interface that allows using the Sql Client API with plain JDBC.
 */
@VertxGen
public interface JDBCPool extends Pool {

  /**
   * The property to be used to retrieve the generated keys
   */
  PropertyKind<Row> GENERATED_KEYS = () -> Row.class;

  /**
   * The property to be used to retreive the output of the callable statement
   */
  PropertyKind<Boolean> OUTPUT = () -> Boolean.class;

  /**
   * Create a JDBC pool which maintains its own data source.
   *
   * @param vertx  the Vert.x instance
   * @param connectOptions the options to configure the connection
   * @param poolOptions the connection pool options
   * @return the client
   */
  static JDBCPool pool(Vertx vertx, SqlConnectOptions connectOptions, PoolOptions poolOptions) {
    final ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    final JDBCConnectOptions jdbcOptions = (JDBCConnectOptions) connectOptions;

    return new JDBCPoolImpl(
      vertx,
      new JDBCClientImpl(vertx, new AgroalCPDataSourceProvider(jdbcOptions, poolOptions)),
      context.tracer() == null ?
        null :
        new QueryTracer(context.tracer(), jdbcOptions.getJdbcUrl(), jdbcOptions.getUser(), jdbcOptions.getDatabase()));
  }

  /**
   * Create a JDBC pool which maintains its own data source.
   *
   * @param vertx  the Vert.x instance
   * @param config the options to configure the client using the same format as {@link io.vertx.ext.jdbc.JDBCClient}
   * @return the client
   */
  static JDBCPool pool(Vertx vertx, JsonObject config) {
    final ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

    return new JDBCPoolImpl(
      vertx,
      new JDBCClientImpl(vertx, config, UUID.randomUUID().toString()),
      context.tracer() == null ?
        null :
        new QueryTracer(context.tracer(), config.getString("jdbcUrl"), config.getString("user"), config.getString("database")));
  }
}
