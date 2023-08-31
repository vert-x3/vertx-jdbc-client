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
package io.vertx.jdbcclient.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.ext.jdbc.impl.JDBCClientImpl;
import io.vertx.ext.jdbc.impl.JDBCConnectionImpl;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.impl.SqlClientBase;
import io.vertx.sqlclient.impl.SqlConnectionBase;
import io.vertx.sqlclient.impl.command.CommandBase;

import java.util.function.Function;

public class JDBCPoolImpl extends SqlClientBase implements JDBCPool {

  private final VertxInternal vertx;
  private final JDBCClientImpl client;
  private final SQLOptions sqlOptions;
  private final String user;
  private final String database;

  public JDBCPoolImpl(Vertx vertx, JDBCClientImpl client, SQLOptions sqlOptions, String user, String database) {
    super(FakeDriver.INSTANCE);
    this.vertx = (VertxInternal) vertx;
    this.client = client;
    this.sqlOptions = sqlOptions;
    this.user = user;
    this.database = database;
  }

  @Override
  protected ContextInternal context() {
    return vertx.getOrCreateContext();
  }

  @Override
  public Future<SqlConnection> getConnection() {
    ContextInternal ctx = vertx.getOrCreateContext();
    return getConnectionInternal(ctx);
  }

  private Future<SqlConnection> getConnectionInternal(ContextInternal ctx) {
    return client
      .getConnection(ctx)
      .map(c -> new SqlConnectionBase<>(ctx, null, new ConnectionImpl(client.getHelper(), ctx, sqlOptions, (JDBCConnectionImpl) c, user, database), driver));
  }

  @Override
  protected <T> PromiseInternal<T> promise() {
    return vertx.promise();
  }

  @Override
  public Future<Void> close() {
    final Promise<Void> promise = vertx.promise();
    client.close(promise);

    return promise.future();
  }

  @Override
  public <R> Future<R> schedule(ContextInternal contextInternal, CommandBase<R> commandBase) {
    ContextInternal ctx = vertx.getOrCreateContext();
    return getConnectionInternal(ctx).flatMap(conn -> ((SqlConnectionBase<?>) conn).schedule(ctx, commandBase).eventually(() -> conn.close()));
  }

  @Override
  public Pool connectHandler(Handler<SqlConnection> handler) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public Pool connectionProvider(Function<Context, Future<SqlConnection>> function) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public int size() {
    return 0;
  }
}
