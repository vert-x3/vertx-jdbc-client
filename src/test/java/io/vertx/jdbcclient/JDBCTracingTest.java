package io.vertx.jdbcclient;

import io.vertx.core.Vertx;
import io.vertx.ext.jdbc.JDBCClientTestBase;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.tck.TracingTestBase;
import org.hsqldb.Server;
import org.junit.Assume;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class JDBCTracingTest extends TracingTestBase {

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
      { 0, "jdbc:hsqldb:hsql://localhost/xdb"},
      { 0, "jdbc:hsqldb:hsql://localhost:9001/xdb"},
      { 0, "jdbc:hsqldb:hsql://127.0.0.1/xdb"},
      { 0, "jdbc:hsqldb:hsql://127.0.0.1:9001/xdb"},
      { 1, "jdbc:hsqldb:mem:" + ClientTestBase.class.getSimpleName() + "?shutdown=true"},
    });
  }

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

  private Pool pool;
  private Server server;
  private final String connectionURL;
  private final int serverMode;

  public JDBCTracingTest(int serverMode, String connectionURL) {
    this.serverMode = serverMode;
    this.connectionURL = connectionURL;
  }

  @Override
  public void setup() throws Exception {
    switch (serverMode) {
      case 0:
        Path dbPath = Files.createTempDirectory("hsqldb-");
        server = new Server();
        server.setDatabaseName(0, "xdb");
        server.setDatabasePath(0, "file:" + dbPath.toString());
        server.setPort(9001);
        server.start();
        Connection conn = DriverManager.getConnection(connectionURL, "SA", "");
        for (String statement : SQL) {
          conn.createStatement().execute(statement);
        }
        break;
      case 1:
        JDBCClientTestBase.resetDb(ClientTestBase.class, SQL);
        break;
      default:
        throw new UnsupportedOperationException();
    }
    super.setup();
  }

  @Override
  public void teardown(TestContext ctx) {
    Pool p = pool;
    pool = null;
    if (p != null) {
      try {
        p.close().toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);
      } catch (Exception ignore) {
      }
    }
    super.teardown(ctx);
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  @Override
  protected Pool createPool(Vertx vertx) {
    if (pool == null) {
      pool = JDBCPool.pool(vertx, new JDBCConnectOptions()
        .setJdbcUrl(connectionURL).setUser("SA").setPassword(""), new PoolOptions());
    }
    return pool;
  }

  @Override
  protected boolean isValidDbSystem(String dbSystem) {
    return "other_sql".equals(dbSystem);
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
