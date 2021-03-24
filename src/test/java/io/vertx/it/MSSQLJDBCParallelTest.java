package io.vertx.it;

import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class MSSQLJDBCParallelTest extends MSSQLTestBase {

  public MSSQLJDBCParallelTest() {
    poolSize = 10;
    isJdbcPool = false;
  }

  @Test(timeout = 15000)
  public void parallelQueriesPoolTest(TestContext should) {
    int n = 100;
    final Async test = should.async(n);
    for (int i = 1; i <= n; i++) {
      sqlClient
        .getConnection(res -> {
          if (res.succeeded()) {
            SQLConnection conn = res.result();
            conn.query("WAITFOR DELAY '00:00:01'", query -> {
              test.countDown();
              conn.close();
            });
          } else {
            should.fail(res.cause());
          }
        });
    }
  }

}
