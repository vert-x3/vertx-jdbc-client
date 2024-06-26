package io.vertx.jdbcclient.impl;

import io.vertx.core.Future;
import io.vertx.core.internal.ContextInternal;
import io.vertx.jdbcclient.JDBCConnection;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.internal.Connection;
import io.vertx.sqlclient.internal.SqlConnectionBase;
import io.vertx.sqlclient.internal.SqlConnectionInternal;
import io.vertx.sqlclient.spi.ConnectionFactory;
import io.vertx.sqlclient.spi.Driver;

public class JDBCConnectionImpl extends SqlConnectionBase<JDBCConnectionImpl> implements JDBCConnection {

  static ConnectionImpl implOf(SqlConnection conn) {
    io.vertx.sqlclient.internal.Connection internal = ((SqlConnectionInternal) conn).unwrap();
    if (!(internal instanceof ConnectionImpl)) {
      // Not pooled
      internal = internal.unwrap();
    }
    return (ConnectionImpl) internal;
  }

  public JDBCConnectionImpl(ContextInternal context, ConnectionFactory factory, Connection conn, Driver driver) {
    super(context, factory, conn, driver);
  }

  @Override
  public JDBCConnection setQueryTimeout(int timeoutInSeconds) {
    implOf(this).sqlOptions.setQueryTimeout(timeoutInSeconds);
    return this;
  }

  public Future<Integer> getTransactionIsolation() {
    return schedule(context, new GetTransactionIsolation());
  }

  public Future<Void> setTransactionIsolation(int isolationLevel) {
    return schedule(context, new SetTransactionIsolation(isolationLevel));
  }
}
