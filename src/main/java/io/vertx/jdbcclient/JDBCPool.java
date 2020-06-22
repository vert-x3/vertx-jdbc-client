package io.vertx.jdbcclient;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.impl.JDBCClientImpl;
import io.vertx.jdbcclient.impl.AgroalCPDataSourceProvider;
import io.vertx.jdbcclient.impl.JDBCPoolImpl;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Row;

import java.util.UUID;

@VertxGen
public interface JDBCPool extends Pool {

  PropertyKind<Row> GENERATED_KEYS = () -> Row.class;
  PropertyKind<Boolean> OUTPUT = () -> Boolean.class;

  /**
   * Create a JDBC pool which maintains its own data source.
   *
   * @param vertx  the Vert.x instance
   * @return the client
   */
  static JDBCPool pool(Vertx vertx, JDBCConnectOptions connectOptions, PoolOptions poolOptions) {
    return new JDBCPoolImpl(
      vertx,
      new JDBCClientImpl(vertx, new AgroalCPDataSourceProvider(connectOptions, poolOptions)),
      // TODO: tracer?
      null);
  }

  static JDBCPool pool(Vertx vertx, JsonObject config) {
    return new JDBCPoolImpl(
      vertx,
      new JDBCClientImpl(vertx, config, UUID.randomUUID().toString()),
      // TODO: tracer?
      null);
  }
}
