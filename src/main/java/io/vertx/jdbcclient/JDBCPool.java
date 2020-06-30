package io.vertx.jdbcclient;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;
import io.vertx.ext.jdbc.impl.JDBCClientImpl;
import io.vertx.jdbcclient.impl.AgroalCPDataSourceProvider;
import io.vertx.jdbcclient.impl.JDBCPoolImpl;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.impl.tracing.QueryTracer;

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
  static JDBCPool pool(Vertx vertx, SqlConnectOptions connectOptions, PoolOptions poolOptions) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    QueryTracer tracer = context.tracer() == null ? null : new QueryTracer(context.tracer(), connectOptions);

    return new JDBCPoolImpl(
      vertx,
      new JDBCClientImpl(vertx, new AgroalCPDataSourceProvider((JDBCConnectOptions) connectOptions, poolOptions)),
      tracer);
  }

  static JDBCPool pool(Vertx vertx, JsonObject config) {

    return new JDBCPoolImpl(
      vertx,
      new JDBCClientImpl(vertx, config, UUID.randomUUID().toString()),
      // TODO: tracer?
      null);
  }
}
