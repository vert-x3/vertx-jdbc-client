package io.vertx.ext.jdbc.impl.actions;

import java.sql.Connection;
import java.sql.SQLException;

public interface JDBCAction<T> {

  T execute(Connection conn) throws SQLException;

}
