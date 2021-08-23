package io.vertx.jdbcclient;

import io.vertx.core.Context;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.sqlclient.PoolOptions;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ConcurrencyTest extends ClientTestBase {

  //TODO: H2 v1.4.200 remove MULTI_THREADED
  //http://www.h2database.com/html/changelog.html?highlight=MULTI_THREADED&search=MULTI_THREADED#firstFound
  @Override
  protected JDBCConnectOptions connectOptions() {
    String url = "jdbc:h2:mem:test-" + JDBCConnectOptions.class.getSimpleName() + ";DB_CLOSE_DELAY=-1";
    //url += ";MULTI_THREADED=1";
    return new JDBCConnectOptions().setJdbcUrl(url);
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
    for (int i = 0; i < n; i++) {
      client.withConnection(conn -> {
        should.assertEquals(ctx, vertx.getOrCreateContext());
        return conn.query("SELECT SLEEP(1000)").execute();
      }).onComplete(should.asyncAssertSuccess(v -> test.countDown()));
    }
    // At most 1 sec
    test.awaitSuccess(2_000);
  }
}
