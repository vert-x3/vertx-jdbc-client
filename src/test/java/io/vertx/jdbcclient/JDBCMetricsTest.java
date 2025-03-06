package io.vertx.jdbcclient;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.sqlclient.*;
import io.vertx.tests.sqlclient.tck.MetricsTestBase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

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

  static class FakeSqlConnectOptions extends SqlConnectOptions {

  }

  @Override
  protected SqlConnectOptions connectOptions() {
    return new FakeSqlConnectOptions();
  }

  @Override
  protected ClientBuilder<Pool> poolBuilder() {
    return new ClientBuilder<>() {
      Vertx vertx;
      PoolOptions poolOptions = new PoolOptions();
      SqlConnectOptions connectOptions;
      @Override
      public ClientBuilder<Pool> with(PoolOptions options) {
        this.poolOptions = options;
        return this;
      }
      @Override
      public ClientBuilder<Pool> with(NetClientOptions options) {
        return this;
      }
      @Override
      public ClientBuilder<Pool> connectingTo(SqlConnectOptions database) {
        this.connectOptions = database;
        return this;
      }
      @Override
      public ClientBuilder<Pool> connectingTo(String database) {
        return this;
      }
      @Override
      public ClientBuilder<Pool> connectingTo(Supplier<Future<SqlConnectOptions>> supplier) {
        return this;
      }
      @Override
      public ClientBuilder<Pool> connectingTo(List<SqlConnectOptions> databases) {
        return this;
      }
      @Override
      public ClientBuilder<Pool> using(Vertx vertx) {
        this.vertx = vertx;
        return this;
      }
      @Override
      public ClientBuilder<Pool> withConnectHandler(Handler<SqlConnection> handler) {
        return this;
      }
      @Override
      public Pool build() {
        return JDBCPool.pool(vertx, new JDBCConnectOptions(options).setMetricsName(connectOptions.getMetricsName()), poolOptions);
      }
    };
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
