package io.vertx.jdbcclient.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.ext.jdbc.impl.JDBCClientImpl;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.PoolBase;
import io.vertx.sqlclient.impl.SqlConnectionImpl;

public class JDBCPoolImpl extends PoolBase<JDBCPoolImpl> implements JDBCPool {

  private VertxInternal vertx;
  private JDBCClientImpl client;

  public JDBCPoolImpl(Vertx vertx, JDBCClientImpl client) {
    super((VertxInternal) vertx, true);
    this.vertx = (VertxInternal) vertx;
    this.client = client;
  }

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
      .setHandler(completionHandler);
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
}
