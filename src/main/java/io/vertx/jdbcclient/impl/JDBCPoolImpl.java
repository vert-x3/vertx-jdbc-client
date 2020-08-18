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
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.impl.SqlClientBase;
import io.vertx.sqlclient.impl.SqlConnectionImpl;
import io.vertx.sqlclient.impl.command.CommandBase;
import io.vertx.sqlclient.impl.tracing.QueryTracer;

public class JDBCPoolImpl extends SqlClientBase<JDBCPoolImpl> implements JDBCPool {

  private final VertxInternal vertx;
  private final JDBCClientImpl client;
  private final SQLOptions queryOptions;

  public JDBCPoolImpl(Vertx vertx, JDBCClientImpl client, QueryTracer tracer) {
    super(tracer, null);
    this.vertx = (VertxInternal) vertx;
    this.client = client;
    // need to get access to the options to create a SQLOptions
    queryOptions = new SQLOptions();
  }

  @Override
  public void getConnection(Handler<AsyncResult<SqlConnection>> handler) {
    getConnection().onComplete(handler);
  }

  @Override
  public Future<SqlConnection> getConnection() {
    ContextInternal ctx = vertx.getOrCreateContext();
    return getConnectionInternal(ctx);
  }

  private Future<SqlConnection> getConnectionInternal(ContextInternal ctx) {
    return client
      .<SqlConnection>getConnection(ctx)
      .map(c -> new SqlConnectionImpl<>(ctx, new ConnectionImpl(client.getHelper(), ctx, queryOptions, (JDBCConnectionImpl) c), tracer, null));
  }

  @Override
  protected <T> PromiseInternal<T> promise() {
    return vertx.promise();
  }

  @Override
  protected <T> PromiseInternal<T> promise(Handler<AsyncResult<T>> handler) {
    return vertx.promise(handler);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    client.close(handler);
  }

  @Override
  public Future<Void> close() {
    final Promise<Void> promise = vertx.promise();
    client.close(promise);

    return promise.future();
  }

  @Override
  public <R> void schedule(CommandBase<R> commandBase, Promise<R> promise) {
    ContextInternal ctx = vertx.getOrCreateContext();
    getConnectionInternal(ctx).flatMap(conn -> {
      Promise<R> p = ctx.promise();
      ((SqlConnectionImpl<?>) conn).schedule(commandBase, p);
      return p.future().flatMap(r -> conn.close().map(r));
    }).onComplete(promise);
  }
}
