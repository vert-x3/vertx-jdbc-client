package io.vertx.it;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.OracleContainer;

/**
 * @author <a href="mailto:Fyro-Ing@users.noreply.github.com">Fyro</a>
 */
@RunWith(VertxUnitRunner.class)
public class OracleTest {

  @Rule
  public final RunTestOnContext rule = new RunTestOnContext();

  private OracleContainer server;
  protected JDBCPool client;

  @Before
  public void before(TestContext should) {
    final Async test = should.async();
    rule.vertx().executeBlocking(p -> {
      try {
        server = new OracleContainer();
        server.withInitScript("init-jdbc.sql");
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

  @Test
  public void simpleDeleteTest(TestContext should) {
    final Async test = should.async();
    client
    .preparedQuery("DELETE FROM product WHERE id = ?")
      .execute(Tuple.of("anId"),should.asyncAssertSuccess(resultSet -> {
        should.assertEquals(1, resultSet.rowCount());
        test.complete();
      }));
  }

  @Test
  public void simpleSelectTest(TestContext should) {
    final Async test = should.async();
    client
    .preparedQuery("SELECT * FROM product")
      .execute(should.asyncAssertSuccess(resultSet -> {
        should.assertEquals(1, resultSet.size());
        test.complete();
      }));
  }

  @Test
  public void simpleInsertTest(TestContext should) {
    final Async test = should.async();
    client
    .preparedQuery("INSERT INTO product(id, name, url, company, area, category, channel, active) VALUES ('id', 'name', 'url', 'company', 'area', 'category', 'channel', 1)")
      .execute(should.asyncAssertSuccess(resultSet -> {
        should.assertEquals(1, resultSet.rowCount());
        test.complete();
      }));
  }

  @Test
  public void simpleUpdateTest(TestContext should) {
    final Async test = should.async();
    client
    .preparedQuery("UPDATE product SET name='aName' WHERE id = 'anId'")
      .execute(should.asyncAssertSuccess(resultSet -> {
        should.assertEquals(1, resultSet.rowCount());
        test.complete();
      }));
  }

}
