package io.vertx.jdbcclient;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

@VertxGen
public interface JDBCConnection extends SqlConnection {

  Future<Integer> getTransactionIsolation();

  Future<Void> setTransactionIsolation(int isolationLevel);

  /**
   * Sets a connection wide query timeout.
   *
   * It can be over-written at any time and becomes active on the next query call.
   *
   * @param timeoutInSeconds the max amount of seconds the query can take to execute.
   */
  @Fluent
  JDBCConnection setQueryTimeout(int timeoutInSeconds);
}
