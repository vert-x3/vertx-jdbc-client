package io.vertx.it;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class MSSQLParallelTest extends MSSQLTestBase {

  public MSSQLParallelTest() {
    poolSize = 10;
    isJdbcPool = true;
  }

  @Test(timeout = 15000)
  public void parallelQueriesPoolTest(TestContext should) {
    int n = 100;
    final Async test = should.async(n);
    for (int i = 1; i <= n; i++) {
      client
        .getConnection()
        .onSuccess(conn -> {
          conn.query("WAITFOR DELAY '00:00:01'")
            .execute()
            .onSuccess(rows -> {
              conn.close();
              test.countDown();
            })
            .onFailure(t -> {
              conn.close();
              should.fail(t);
            });
        })
        .onFailure(t -> should.fail(t))
      ;
    }
  }

}
