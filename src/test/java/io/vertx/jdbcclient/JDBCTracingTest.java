package io.vertx.jdbcclient;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.jdbc.DBConfigs;
import io.vertx.ext.jdbc.JDBCClientTestBase;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.tck.TracingTestBase;
import org.junit.Assume;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@RunWith(VertxUnitRunner.class)
public class JDBCTracingTest extends TracingTestBase {

  private static final List<String> SQL;

  static {
    SQL = Arrays.asList(("DROP TABLE IF EXISTS immutable;\n" +
      "CREATE TABLE immutable (id integer NOT NULL, message varchar(2048) NOT NULL, PRIMARY KEY (id));\n" +
      "INSERT INTO immutable (id, message) VALUES (1, 'fortune: No such file or directory');\n" +
      "INSERT INTO immutable (id, message) VALUES (2, 'A computer scientist is someone who fixes things that aren''t broken.');\n" +
      "INSERT INTO immutable (id, message) VALUES (3, 'After enough decimal places, nobody gives a damn.');\n" +
      "INSERT INTO immutable (id, message) VALUES (4, 'A bad random number generator: 1, 1, 1, 1, 1, 4.33e+67, 1, 1, 1');\n" +
      "INSERT INTO immutable (id, message) VALUES (5, 'A computer program does what you tell it to do, not what you want it to do.');\n" +
      "INSERT INTO immutable (id, message) VALUES (6, 'Emacs is a nice operating system, but I prefer UNIX. — Tom Christaensen');\n" +
      "INSERT INTO immutable (id, message) VALUES (7, 'Any program that runs right is obsolete.');\n" +
      "INSERT INTO immutable (id, message) VALUES (8, 'A list is only as strong as its weakest link. — Donald Knuth');\n" +
      "INSERT INTO immutable (id, message) VALUES (9, 'Feature: A bug with seniority.');\n" +
      "INSERT INTO immutable (id, message) VALUES (10, 'Computers make very fast, very accurate mistakes.');\n" +
      "INSERT INTO immutable (id, message) VALUES (11, '<script>alert(\"This should not be displayed in a browser alert box.\");</script>');\n" +
      "INSERT INTO immutable (id, message) VALUES (12, 'フレームワークのベンチマーク');").split("\n"));

  }

  private JDBCPool pool;

  @Override
  public void setup() throws Exception {
    JDBCClientTestBase.resetDb(ClientTestBase.class, SQL);
    super.setup();
  }

  @Override
  public void teardown(TestContext ctx) {
    JDBCPool p = pool;
    pool = null;
    if (p != null) {
      try {
        p.close().toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);
      } catch (Exception ignore) {
      }
    }
    super.teardown(ctx);
  }

  @Override
  protected Pool createPool(Vertx vertx) {
    if (pool == null) {
      pool = JDBCPool.pool(vertx, new JDBCConnectOptions()
        .setJdbcUrl(DBConfigs.hsqldb(ClientTestBase.class).getString("url")), new PoolOptions());
    }
    return pool;
  }

  @Override
  protected String statement(String... parts) {
    return String.join("?", parts);
  }

  @Override
  public void testTraceBatchQuery(TestContext ctx) {
    Assume.assumeTrue(false);
  }

  @Override
  public void testTracePooledBatchQuery(TestContext ctx) {
    Assume.assumeTrue(false);
  }
}
