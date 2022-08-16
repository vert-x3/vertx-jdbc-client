package io.vertx.jdbcclient;

import io.vertx.jdbcclient.impl.ConnectionImpl;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.impl.SqlConnectionInternal;

public class JDBCUtils {

  /**
   * Unwrap a Vert.x SQL connection to a JDBC connection.
   *
   * @param conn the Vert.x connection
   * @return the JDBC connection
   */
  static java.sql.Connection unwrap(SqlConnection conn) {
    ConnectionImpl impl = (ConnectionImpl) ((SqlConnectionInternal) conn).unwrap();
    return impl.getJDBCConnection();
  }
}
