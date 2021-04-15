package io.vertx.jdbcclient;

import io.vertx.core.Context;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.sqlclient.PoolOptions;
import org.junit.Test;

public class ConcurrencyTest extends ClientTestBase {

  @Override
  protected JDBCConnectOptions connectOptions() {
    return new JDBCConnectOptions()
      .setJdbcUrl("jdbc:h2:mem:test-" + JDBCConnectOptions.class.getSimpleName() + ";DB_CLOSE_DELAY=-1;MULTI_THREADED=1");
  }

  @Override
  protected PoolOptions poolOptions() {
    return super.poolOptions().setMaxSize(10);
  }

  @Test
  public void concurrentQueriesOnSameContext(TestContext should) {
    Async latch = should.async();
    client.query("CREATE ALIAS SLEEP FOR \"io.vertx.ext.jdbc.PoolTest.sleep\";")
      .execute()
      .onComplete(should.asyncAssertSuccess(r -> {
        latch.complete();
      }));
    latch.awaitSuccess(20_000);
    int n = 10;
    Context ctx = vertx.getOrCreateContext();
    final Async test = should.async(n);
    for (int i = 0;i < n;i++) {
      client.withConnection(conn -> {
        should.assertEquals(ctx, vertx.getOrCreateContext());
        return conn.query("SELECT SLEEP(1000)").execute();
      }).onComplete(should.asyncAssertSuccess(v -> test.countDown()));
    }
    // At most 1 sec
    test.awaitSuccess(2_000);
  }
}
