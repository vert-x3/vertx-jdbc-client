package io.vertx.it;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.OracleContainer;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * @author <a href="mailto:Fyro-Ing@users.noreply.github.com">Fyro</a>
 */
@RunWith(VertxUnitRunner.class)
public class OracleTest {

  @ClassRule
  public static final RunTestOnContext rule = new RunTestOnContext();

  private static OracleContainer server;

  @BeforeClass
  public static void setup(TestContext should) {
    final Async test = should.async();
    rule.vertx().executeBlocking(p -> {
      try {
        server = new OracleContainer("wnameless/oracle-xe-11g-r2:latest");
        server.withInitScript("init-oracle.sql");
        server.start();
        p.complete();
      } catch (RuntimeException e) {
        p.fail(e);
      }
    }, true).onSuccess(o -> test.complete()).onFailure(should::fail);
  }

  @AfterClass
  public static void tearDown() {
    server.close();
  }

  @Test
  public void simpleDeleteTest(TestContext should) {
    final Async test = should.async();
    JDBCClient client = initJDBCClient();
    client
      .updateWithParams("DELETE FROM insert_table WHERE id = ?", new JsonArray().add(1),
        should.asyncAssertSuccess(resultSet -> {
          should.assertEquals(1, resultSet.getUpdated());
          test.complete();
        }));
  }

  @Test
  public void simpleSelectTest(TestContext should) {
    final Async async = should.async();
    JDBCClient client = initJDBCClient();
    client
      .query("SELECT * FROM insert_table WHERE id = 2", should.asyncAssertSuccess(resultSet -> {
        Assert.assertEquals(1, resultSet.getNumRows());
        final JsonArray row = resultSet.getResults().get(0);
        Assert.assertEquals(2, (int) row.getInteger(0));
        Assert.assertEquals("hello", row.getValue(1));
        Assert.assertEquals("vertx", row.getValue(2));
        Assert.assertEquals(LocalDateTime.class, row.getValue(3).getClass());
        Assert.assertEquals(LocalDateTime.class, row.getValue(4).getClass());
        async.complete();
      }));
  }

  @Test
  public void simpleInsertTest(TestContext should) {
    final Async async = should.async();
    JDBCClient client = initJDBCClient();
    client
      .updateWithParams("INSERT INTO insert_table VALUES (?, ?, ?, ?, ?)",
        new JsonArray().add(3).add("doe").add("john")
          .add(LocalDateTime.of(2001, 1, 1, 0, 0))
          .add(LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault())),
        should.asyncAssertSuccess(resultSet -> {
          Assert.assertEquals(1, resultSet.getUpdated());
          async.complete();
        }));
  }

  @Test
  public void simpleUpdateTest(TestContext should) {
    final Async async = should.async();
    JDBCClient client = initJDBCClient();
    client
      .updateWithParams("UPDATE insert_table SET lname=?, cdate=? WHERE id = 2",
        new JsonArray().add("aName").add(LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault())),
        should.asyncAssertSuccess(resultSet -> {
          should.assertEquals(1, resultSet.getUpdated());
          async.complete();
        }));
  }

  private JDBCClient initJDBCClient() {
    JsonObject options = new JsonObject()
      .put("url", server.getJdbcUrl())
      .put("user", server.getUsername())
      .put("password", server.getPassword())
      .put("driver_class", "oracle.jdbc.driver.OracleDriver");
    return JDBCClient.createShared(rule.vertx(), options, "dbName");
  }
}
