package io.vertx.jdbcclient.impl;

import io.vertx.jdbcclient.impl.actions.JDBCAction;
import io.vertx.sqlclient.impl.command.CommandBase;

import java.sql.Connection;
import java.sql.SQLException;

public class SetTransactionIsolation extends CommandBase<Void> implements JDBCAction<Void> {

  final int isolationLevel;

  public SetTransactionIsolation(int isolationLevel) {
    this.isolationLevel = isolationLevel;
  }

  @Override
  public Void execute(Connection conn) throws SQLException {
    conn.setTransactionIsolation(isolationLevel);
    return null;
  }
}
