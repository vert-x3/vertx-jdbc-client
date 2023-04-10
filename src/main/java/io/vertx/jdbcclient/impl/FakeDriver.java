package io.vertx.jdbcclient.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.CloseFuture;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.spi.ConnectionFactory;
import io.vertx.sqlclient.spi.Driver;

import java.util.List;
import java.util.function.Supplier;

/**
 * For now fake as we don't use the driver system, perhaps implemented later.
 */
public class FakeDriver implements Driver {

  public static final FakeDriver INSTANCE = new FakeDriver();

  @Override
  public SqlConnectOptions parseConnectionUri(String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Pool newPool(Vertx vertx, Supplier<? extends Future<? extends SqlConnectOptions>> databases, PoolOptions options, CloseFuture closeFuture) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConnectionFactory createConnectionFactory(Vertx vertx, SqlConnectOptions database) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConnectionFactory createConnectionFactory(Vertx vertx, Supplier<? extends Future<? extends SqlConnectOptions>> database) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean acceptsOptions(SqlConnectOptions connectOptions) {
    throw new UnsupportedOperationException();
  }
}
