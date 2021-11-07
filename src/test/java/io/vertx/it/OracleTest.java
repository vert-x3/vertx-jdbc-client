package io.vertx.it;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.time.Instant;
import java.time.LocalDate;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.OracleContainer;

/**
 * @author <a href="mailto:Fyro-Ing@users.noreply.github.com">Fyro</a>
 */
@RunWith(VertxUnitRunner.class)
public class OracleTest {

  @Rule
  public final RunTestOnContext rule = new RunTestOnContext();

  private OracleContainer server;
  protected JDBCClient client;

  @Before
  public void before(TestContext should) {
    final Async test = should.async();
    rule.vertx().executeBlocking(p -> {
      try {
        server = new OracleContainer("wnameless/oracle-xe-11g-r2:latest");
        server.withInitScript("init-jdbc.sql");
        server.start();
        p.complete();
      } catch (RuntimeException e) {
        p.fail(e);
      }
    }, true, init -> {
      if (init.succeeded()) {
        JsonObject options = new JsonObject()
          .put("url", server.getJdbcUrl())
          .put("user", server.getUsername())
          .put("password", server.getPassword())
          .put("driver_class", "oracle.jdbc.driver.OracleDriver");

        // client = JDBCPool.pool(rule.vertx(), options, new PoolOptions().setMaxSize(1));
        client = JDBCClient.createShared(rule.vertx(), options, "dbName");
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
      .updateWithParams("DELETE FROM insert_table WHERE id = ?", new JsonArray().add(1),
        should.asyncAssertSuccess(resultSet -> {
          should.assertEquals(1, resultSet.getUpdated());
          test.complete();
        }));
  }

  @Test
  public void simpleSelectTest(TestContext should) {
    final Async test = should.async();
    client
      .query("SELECT * FROM insert_table", should.asyncAssertSuccess(resultSet -> {
        should.assertEquals(1, resultSet.getNumRows());
        test.complete();
      }));
  }

  @Test
  public void simpleInsertTest(TestContext should) {
    final Async test = should.async();
    client
      .updateWithParams("INSERT INTO insert_table VALUES (?, ?, ?, ?, ?)",
        new JsonArray().add(2).add("doe").add("john").add(LocalDate.of(2001,1,1)).add(Instant.now()),
        should.asyncAssertSuccess(resultSet -> {
          should.assertEquals(1, resultSet.getUpdated());
          test.complete();
        }));
  }

  @Test
  public void simpleUpdateTest(TestContext should) {
    final Async test = should.async();
    client
      .updateWithParams("UPDATE insert_table SET lname=?, cdate=? WHERE id = 1",
        new JsonArray().add("aName").add(Instant.now()),
        should.asyncAssertSuccess(resultSet -> {
          should.assertEquals(1, resultSet.getUpdated());
          test.complete();
        }));
  }

}
