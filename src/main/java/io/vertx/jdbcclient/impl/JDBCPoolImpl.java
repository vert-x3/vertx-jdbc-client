package io.vertx.jdbcclient.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.ext.jdbc.impl.JDBCClientImpl;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.PoolBase;
import io.vertx.sqlclient.impl.SqlClientBase;
import io.vertx.sqlclient.impl.SqlConnectionImpl;
import io.vertx.sqlclient.impl.command.CommandBase;

import java.util.function.Function;

public class JDBCPoolImpl extends SqlClientBase<JDBCPoolImpl> implements JDBCPool {

  private VertxInternal vertx;
  private JDBCClientImpl client;

  public JDBCPoolImpl(Vertx vertx, JDBCClientImpl client) {
    super(null);
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
    return client
      .<SqlConnection>getConnection(ctx)
      .map(c -> (Connection)new ConnectionImpl(client.getHelper(), ctx, (io.vertx.ext.jdbc.impl.JDBCConnectionImpl) c))
      .map(c -> (SqlConnection)new SqlConnectionImpl<>(ctx, c, null));
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

  }

  @Override
  public Future<Void> close() {
    return Future.succeededFuture();
  }

  @Override
  public <R> void schedule(CommandBase<R> commandBase, Promise<R> promise) {

  }

  /*
  @Override
  public void connect(Handler<AsyncResult<Connection>> completionHandler) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void acquire(Handler<AsyncResult<Connection>> completionHandler) {
    ContextInternal ctx = vertx.getOrCreateContext();
    client
      .getConnection(ctx)
      .map(c -> (Connection)new ConnectionImpl(client.getHelper(), ctx, (io.vertx.ext.jdbc.impl.JDBCConnectionImpl) c))
      .onComplete(completionHandler);
  }

  @Override
  protected SqlConnectionImpl<SqlConnection> wrap(ContextInternal context, Connection conn) {
    return new SqlConnectionImpl<>(context, conn);
  }

  @Override
  protected void doClose() {
    client.close();
    super.doClose();
  }
*/
}
