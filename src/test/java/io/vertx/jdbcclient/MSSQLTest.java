package io.vertx.jdbcclient;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.PoolOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MSSQLServerContainer;

@RunWith(VertxUnitRunner.class)
public class MSSQLTest {

  @Rule
  public final RunTestOnContext rule = new RunTestOnContext();

  private MSSQLServer server;
  protected JDBCPool client;

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
        JDBCConnectOptions options = new JDBCConnectOptions()
          .setJdbcUrl(server.getJdbcUrl())
          .setUser(server.getUsername())
          .setPassword(server.getPassword());

        client = JDBCPool.pool(rule.vertx(), options, new PoolOptions().setMaxSize(1));
        test.complete();
      } else {
        should.fail(init.cause());
      }
    });
  }

  @After
  public void after() {
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

  @Test
  public void simpleTest(TestContext should) {
    final Async test = should.async();

    client
      .preparedQuery("select * from Fortune")
      .execute(should.asyncAssertSuccess(resultSet -> {
        should.assertEquals(12, resultSet.size());
        test.complete();
      }));
  }
}
