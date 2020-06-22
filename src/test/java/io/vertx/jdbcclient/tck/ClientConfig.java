package io.vertx.jdbcclient.tck;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.tck.Connector;

public enum ClientConfig {
  POOLED() {
    @Override
    Connector<SqlClient> connect(Vertx vertx, SqlConnectOptions connectOptions) {
      JDBCPool pool = JDBCPool.pool(vertx, (JDBCConnectOptions) connectOptions, new PoolOptions().setMaxSize(1));
      return new Connector<SqlClient>() {
        @Override
        public void connect(Handler<AsyncResult<SqlClient>> handler) {
          pool.getConnection(ar -> {
            if (ar.succeeded()) {
              handler.handle(Future.succeededFuture(ar.result()));
            } else {
              handler.handle(Future.failedFuture(ar.cause()));
            }
          });
        }
        @Override
        public void close() {
          pool.close();
        }
      };
    }
  };

  abstract <C extends SqlClient> Connector<C> connect(Vertx vertx, SqlConnectOptions options);
}
