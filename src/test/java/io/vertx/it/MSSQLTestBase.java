package io.vertx.it;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.testcontainers.containers.MSSQLServerContainer;

public abstract class MSSQLTestBase {

  @Rule
  public final RunTestOnContext rule = new RunTestOnContext();

  private MSSQLServer server;
  protected JDBCPool client;
  protected SQLClient sqlClient;
  protected int poolSize = 1;
  protected boolean isJdbcPool = true;

  @Before
  public void before(TestContext should) {
    final Async test = should.async();
    rule.vertx().executeBlocking(p -> {
      try {
        server = new MSSQLServer();
        server.withInitScript("init-mssql.sql");
        server.start();
        p.complete();
      } catch (RuntimeException e) {
        p.fail(e);
      }
    }, true, init -> {
      if (init.succeeded()) {
        if (isJdbcPool) {
          JDBCConnectOptions options = new JDBCConnectOptions()
            .setJdbcUrl(server.getJdbcUrl())
            .setUser(server.getUsername())
            .setPassword(server.getPassword());

          client = JDBCPool.pool(rule.vertx(), options, new PoolOptions().setMaxSize(poolSize));
        } else {
          sqlClient = JDBCClient.create(rule.vertx(), new JsonObject()
            .put("url", server.getJdbcUrl())
            .put("user", server.getUsername())
            .put("password", server.getPassword())
            .put("driver_class", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .put("max_pool_size", poolSize));
        }
        test.complete();
      } else {
        should.fail(init.cause());
      }
    });
  }

  @After
  public void after() {
    if (sqlClient != null) {
      sqlClient.close();
    }
    if (client != null) {
      client.close();
    }
    server.close();
  }

  private static class MSSQLServer extends MSSQLServerContainer {
    @Override
    protected void configure() {
      this.addExposedPort(MSSQLServerContainer.MS_SQL_SERVER_PORT);
      this.addEnv("ACCEPT_EULA", "Y");
      this.addEnv("SA_PASSWORD", this.getPassword());
    }
  }

}
