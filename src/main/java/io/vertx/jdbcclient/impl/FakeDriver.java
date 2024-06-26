package io.vertx.jdbcclient.impl;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.internal.CloseFuture;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.net.NetClientOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.internal.Connection;
import io.vertx.sqlclient.internal.SqlConnectionInternal;
import io.vertx.sqlclient.spi.ConnectionFactory;
import io.vertx.sqlclient.spi.Driver;

import java.util.function.Supplier;

/**
 * For now fake as we don't use the driver system, perhaps implemented later.
 */
public class FakeDriver implements Driver<SqlConnectOptions> {

  public static final FakeDriver INSTANCE = new FakeDriver();

  @Override
  public SqlConnectOptions parseConnectionUri(String s) {
    throw new UnsupportedOperationException();
  }


  @Override
  public Pool newPool(Vertx vertx, Supplier<Future<SqlConnectOptions>> databases, PoolOptions options, NetClientOptions transportOptions, Handler<SqlConnection> connectHandler, CloseFuture closeFuture) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConnectionFactory<SqlConnectOptions> createConnectionFactory(Vertx vertx, NetClientOptions transportOptions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SqlConnectOptions downcast(SqlConnectOptions connectOptions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean acceptsOptions(SqlConnectOptions connectOptions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SqlConnectionInternal wrapConnection(ContextInternal context, ConnectionFactory<SqlConnectOptions> factory, Connection conn) {
    return new JDBCConnectionImpl(context, factory, conn, FakeDriver.INSTANCE);
  }
}
