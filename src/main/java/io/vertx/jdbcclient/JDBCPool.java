package io.vertx.jdbcclient;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.impl.JDBCClientImpl;
import io.vertx.jdbcclient.impl.JDBCPoolImpl;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlClient;

import java.util.UUID;
import java.util.function.Function;

@VertxGen
public interface JDBCPool extends Pool {

  /**
   * Create a JDBC pool which maintains its own data source.
   *
   * @param vertx  the Vert.x instance
   * @param config the configuration
   * @return the client
   */
  static JDBCPool create(Vertx vertx, JsonObject config) {
    return new JDBCPoolImpl(
      vertx,
      new JDBCClientImpl(vertx, config, UUID.randomUUID().toString()),
      // TODO: tracer?
      null);
  }

  @Override
  default <T> void withTransaction(Function<SqlClient, Future<T>> function, Handler<AsyncResult<T>> handler) {
    Pool.super.withTransaction(function, handler);
  }

  @Override
  default <T> Future<T> withTransaction(Function<SqlClient, Future<T>> function) {
    return Pool.super.withTransaction(function);
  }
}
