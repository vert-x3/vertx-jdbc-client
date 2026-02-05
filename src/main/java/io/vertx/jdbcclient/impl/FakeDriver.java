package io.vertx.jdbcclient.impl;

import io.vertx.core.Completable;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.net.NetClientOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.internal.SqlConnectionInternal;
import io.vertx.sqlclient.spi.DriverBase;
import io.vertx.sqlclient.spi.connection.Connection;
import io.vertx.sqlclient.spi.connection.ConnectionFactory;

import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * For now fake as we don't use the driver system, perhaps implemented later.
 */
public class FakeDriver extends DriverBase<FakeSqlConnectOptions> {

  private static final Function<Connection, Future<Void>> AFTER_ACQUIRE = conn -> {
    ConnectionImpl jdbc = (ConnectionImpl) (conn).unwrap();
    return jdbc.beforeUsage();
  };

  private static final Function<Connection, Future<Void>> BEFORE_RECYCLE = conn -> {
    ConnectionImpl jdbc = (ConnectionImpl) (conn).unwrap();
    return jdbc.afterUsage();
  };

  final Callable<java.sql.Connection> connectionFactory;

  public FakeDriver(Callable<java.sql.Connection> connectionFactory) {
    super("jdbcclient", AFTER_ACQUIRE, BEFORE_RECYCLE);
    this.connectionFactory = connectionFactory;
  }

  @Override
  public SqlConnectOptions parseConnectionUri(String s) {
    throw new UnsupportedOperationException();
  }


  @Override
  public ConnectionFactory<FakeSqlConnectOptions> createConnectionFactory(Vertx vertx, NetClientOptions transportOptions) {
    return new ConnectionFactory<>() {
      @Override
      public Future<Connection> connect(Context context, FakeSqlConnectOptions options) {
        return new JDBCPoolImpl.ConnectionFactory((VertxInternal) vertx, options.actual, connectionFactory).connect((ContextInternal) context);
      }
      @Override
      public void close(Completable<Void> completion) {
        completion.succeed();
      }
    };
  }

  @Override
  public FakeSqlConnectOptions downcast(SqlConnectOptions connectOptions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean acceptsOptions(SqlConnectOptions connectOptions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SqlConnectionInternal wrapConnection(ContextInternal context, ConnectionFactory<FakeSqlConnectOptions> factory, Connection conn) {
    return new JDBCConnectionImpl(context, factory, conn, this);
  }
}
