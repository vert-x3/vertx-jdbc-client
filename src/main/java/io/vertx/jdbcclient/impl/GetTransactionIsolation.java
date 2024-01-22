package io.vertx.jdbcclient.impl;

import io.vertx.ext.jdbc.impl.actions.JDBCAction;
import io.vertx.sqlclient.impl.command.CommandBase;

import java.sql.Connection;
import java.sql.SQLException;

public class GetTransactionIsolation extends CommandBase<Integer> implements JDBCAction<Integer> {

  public GetTransactionIsolation() {
  }

  @Override
  public Integer execute(Connection conn) throws SQLException {
    return conn.getTransactionIsolation();
  }
}
