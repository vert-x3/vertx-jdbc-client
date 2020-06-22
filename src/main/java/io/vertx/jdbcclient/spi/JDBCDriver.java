package io.vertx.jdbcclient.spi;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.spi.Driver;

public class JDBCDriver implements Driver {
  @Override
  public Pool createPool(SqlConnectOptions sqlConnectOptions, PoolOptions poolOptions) {
    if (Vertx.currentContext() != null) {
      throw new IllegalStateException("Running in a Vertx context => use JDBCPool#pool(Vertx, JDBCConnectOptions, PoolOptions) instead");
    }
    VertxOptions vertxOptions = new VertxOptions();
    Vertx vertx = Vertx.vertx(vertxOptions);

    return createPool(vertx, sqlConnectOptions, poolOptions);
  }

  @Override
  public Pool createPool(Vertx vertx, SqlConnectOptions sqlConnectOptions, PoolOptions poolOptions) {
    return JDBCPool.pool(
      vertx,
      (JDBCConnectOptions) sqlConnectOptions,
      poolOptions);
  }

  @Override
  public boolean acceptsOptions(SqlConnectOptions options) {
    if (options instanceof JDBCConnectOptions) {
      return true;
    }

    if (options != null) {
      return SqlConnectOptions.class.equals(options.getClass());
    }

    return false;
  }
}
