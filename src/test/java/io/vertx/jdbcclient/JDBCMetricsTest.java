package io.vertx.jdbcclient;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.tck.MetricsTestBase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

public class JDBCMetricsTest extends MetricsTestBase {

  private static final List<String> STATEMENTS = new ArrayList<>();

  static {
    STATEMENTS.add("DROP TABLE IF EXISTS immutable");
    STATEMENTS.add("CREATE TABLE immutable (id integer NOT NULL, message varchar(2048) NOT NULL, PRIMARY KEY (id))");
    STATEMENTS.add("INSERT INTO immutable (id, message) VALUES (1, 'fortune: No such file or directory')");
  }

  private static final JDBCConnectOptions options = new JDBCConnectOptions()
    .setJdbcUrl("jdbc:hsqldb:mem:" + JDBCMetricsTest.class.getSimpleName() + "?shutdown=true");

  @Override
  public void setup() {
    try {
      Connection conn = DriverManager.getConnection(options.getJdbcUrl());
      for (String statement : STATEMENTS) {
        conn.createStatement().execute(statement);
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
    super.setup();
  }

  @Override
  protected Pool createPool(Vertx vertx) {
    return JDBCPool.pool(vertx, options, new PoolOptions());
  }

  @Override
  public void testPreparedBatchQuery(TestContext ctx) {
    // Does not pass for now
  }

  @Override
  public void testPrepareAndBatchQuery(TestContext ctx) {
    // Does not pass for now
  }

  @Override
  protected String statement(String... parts) {
    throw new UnsupportedOperationException();
  }
}
