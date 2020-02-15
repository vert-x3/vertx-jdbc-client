package io.vertx.jdbcclient;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.impl.JDBCClientImpl;
import io.vertx.jdbcclient.impl.JDBCPoolImpl;
import io.vertx.sqlclient.Pool;

import java.util.UUID;

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
    return new JDBCPoolImpl(vertx, new JDBCClientImpl(vertx, config, UUID.randomUUID().toString()));
  }
}
