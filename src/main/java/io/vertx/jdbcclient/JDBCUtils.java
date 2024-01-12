package io.vertx.jdbcclient;

import io.vertx.core.Future;
import io.vertx.ext.jdbc.impl.actions.AbstractJDBCAction;
import io.vertx.ext.jdbc.impl.actions.JDBCAction;
import io.vertx.jdbcclient.impl.ConnectionImpl;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.impl.SqlConnectionInternal;

import java.sql.Connection;
import java.sql.SQLException;

public class JDBCUtils {

  /**
   * Unwrap a Vert.x SQL connection to a JDBC connection.
   *
   * @param conn the Vert.x connection
   * @return the JDBC connection
   */
  static java.sql.Connection unwrap(SqlConnection conn) {
    return implOf(conn).getJDBCConnection();
  }

  private static ConnectionImpl implOf(SqlConnection conn) {
    io.vertx.sqlclient.impl.Connection internal = ((SqlConnectionInternal) conn).unwrap();
    if (!(internal instanceof ConnectionImpl)) {
      // Not pooled
      internal = internal.unwrap();
    }
    return (ConnectionImpl) internal;
  }

  static Future<Integer> getTransactionIsolation(SqlConnection conn) {
    ConnectionImpl impl = implOf(conn);
    return impl.schedule(Connection::getTransactionIsolation);
  }

  static Future<Void> setTransactionIsolation(SqlConnection conn, int isolationLevel) {
    ConnectionImpl impl = implOf(conn);
    return impl.schedule(c -> {
      c.setTransactionIsolation(isolationLevel);
      return null;
    });
  }
}
