package io.vertx.jdbcclient;

import io.vertx.core.AbstractVerticle;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.vertx.ext.jdbc.JDBCClientTestBase.resetDb;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@RunWith(VertxUnitRunner.class)
public class CloseTest extends ClientTestBase {

  @BeforeClass
  public static void createDb() throws Exception {
    resetDb(CloseTest.class);
  }

  private static final JDBCConnectOptions theConfig = DataSourceConfigs.hsqldb(CloseTest.class);

  private static List<Pool> poolsInTest = Collections.synchronizedList(new ArrayList<>());

  public static class ClientVerticle extends AbstractVerticle {

    private final boolean shared;

    public ClientVerticle(boolean shared) {
      this.shared = shared;
    }

    @Override
    public void start(io.vertx.core.Promise<Void> promise) throws Exception {
      Pool pool = JDBCPool.pool(vertx, theConfig, new PoolOptions().setShared(shared));
      poolsInTest.add(pool);
      String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
      pool
        .query(sql)
        .execute()
        .<Void>mapEmpty()
        .onComplete(promise);
    }
  }

  @Override
  public void setUp() throws Exception {
    poolsInTest.clear();
    super.setUp();
  }

  @Test
  public void testUsingNonSharedInVerticle(TestContext should) throws Exception {
    List<String> ids = deployVerticles(false, 1);
    should.assertEquals(1, poolsInTest.size());
    close(should, ids.get(0));
    checkClosed(should, 0);
  }

  @Test
  public void testUsingNonSharedInVerticle2(TestContext should) throws Exception {
    List<String> ids = deployVerticles(false, 2);
    should.assertEquals(2, poolsInTest.size());
    close(should, ids.get(0));
    checkClosed(should, 0);
    checkOpen(should, 1);
    close(should, ids.get(1));
    checkClosed(should, 0);
    checkClosed(should, 1);
  }

  @Test
  public void testUsingSharedInVerticle(TestContext should) throws Exception {
    List<String> ids = deployVerticles(true, 1);
    should.assertEquals(1, poolsInTest.size());
    close(should, ids.get(0));
    checkClosed(should, 0);
  }

  @Test
  public void testUsingSharedInVerticle2(TestContext should) throws Exception {
    List<String> ids = deployVerticles(true, 2);
    should.assertEquals(2, poolsInTest.size());
    close(should, ids.get(0));
    checkOpen(should, 0);
    checkOpen(should, 1);
    close(should, ids.get(1));
    checkClosed(should, 0);
    checkClosed(should, 1);
  }

  private List<String> deployVerticles(boolean shared, int num) throws Exception {
    List<String> ids = new ArrayList<>();
    for (int i = 0;i < num;i++) {
      String id = vertx
        .deployVerticle(new ClientVerticle(shared))
        .toCompletionStage()
        .toCompletableFuture()
        .get();
      ids.add(id);
    }
    return ids;
  }

  private void close(TestContext should, String deploymentId) throws Exception {
    Async closeLatch = should.async();
    vertx.undeploy(deploymentId).onComplete(should.asyncAssertSuccess(v -> {
      closeLatch.countDown();
    }));
    closeLatch.awaitSuccess();
  }

  private void checkOpen(TestContext should, int idx) {
    try {
      poolsInTest.get(idx).getConnection().toCompletionStage().toCompletableFuture().get();
    } catch (InterruptedException e) {
      should.fail(e);
    } catch (ExecutionException e) {
      should.fail(e.getCause());
    }
  }

  private void checkClosed(TestContext should, int idx) {
    try {
      poolsInTest.get(idx).getConnection().toCompletionStage().toCompletableFuture().get();
    } catch (InterruptedException e) {
      should.fail(e);
    } catch (ExecutionException e) {
      return;
    }
    should.fail("Expected closed pool");
  }
}
