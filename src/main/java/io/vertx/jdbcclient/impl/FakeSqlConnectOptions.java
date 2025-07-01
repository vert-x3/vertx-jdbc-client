package io.vertx.jdbcclient.impl;

import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.sqlclient.SqlConnectOptions;

public class FakeSqlConnectOptions extends SqlConnectOptions {

  public JDBCConnectOptions actual;

  public FakeSqlConnectOptions(JDBCConnectOptions actual) {
    this.actual = actual;
  }
}
