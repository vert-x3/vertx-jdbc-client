package io.vertx.jdbcclient;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class DatasourceTest {

  private static final String URL = "jdbc:hsqldb:mem:" + DatasourceTest.class.getSimpleName() + "?shutdown=true";

  @Test
  public void testFromDatasource(TestContext ctx) throws Exception {
    JDBCDataSource dataSource = new JDBCDataSource();
    dataSource.setURL(URL);
    dataSource.setUser("");
    dataSource.setPassword("");
    Vertx vertx = Vertx.vertx();
    Pool pool = JDBCPool.pool(vertx, dataSource, new PoolOptions());
    Async async = ctx.async();
    try {
      pool
        .withConnection(conn -> Future.succeededFuture())
        .onComplete(ctx.asyncAssertSuccess(v -> async.complete()));
      async.awaitSuccess();
    } finally {
      vertx.close();
    }
  }
}
