package io.vertx.jdbcclient.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.ext.jdbc.impl.JDBCClientImpl;
import io.vertx.ext.jdbc.impl.JDBCConnectionImpl;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.PoolBase;
import io.vertx.sqlclient.impl.SqlClientBase;
import io.vertx.sqlclient.impl.SqlConnectionImpl;
import io.vertx.sqlclient.impl.command.CommandBase;
import io.vertx.sqlclient.impl.tracing.QueryTracer;

import java.util.function.Function;

public class JDBCPoolImpl extends SqlClientBase<JDBCPoolImpl> implements JDBCPool {

  private final VertxInternal vertx;
  private final JDBCClientImpl client;

  public JDBCPoolImpl(Vertx vertx, JDBCClientImpl client, QueryTracer tracer) {
    super(tracer);
    this.vertx = (VertxInternal) vertx;
    this.client = client;
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
      .map(c -> new SqlConnectionImpl<>(ctx, new ConnectionImpl(client.getHelper(), ctx, (JDBCConnectionImpl) c), tracer));
  }

  @Override
  protected <T> Promise<T> promise() {
    return vertx.promise();
  }

  @Override
  protected <T> Promise<T> promise(Handler<AsyncResult<T>> handler) {
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
