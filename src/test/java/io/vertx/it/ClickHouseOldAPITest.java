package io.vertx.it;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(VertxUnitRunner.class)
public class ClickHouseOldAPITest {

  private Vertx vertx;
  private ClickHouseContainer container;
  protected JDBCClient client;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    container = new ClickHouseContainer("clickhouse/clickhouse-server:22.8-alpine");
    container.withInitScript("init-clickhouse.sql");
    container.start();
    JsonObject config = new JsonObject()
      .put("driver_class", "ru.yandex.clickhouse.ClickHouseDriver")
      .put("url", "jdbc:clickhouse://localhost:" + container.getMappedPort(8123) + "/default");
    client = JDBCClient.create(vertx, config);
  }

  @After
  public void after(TestContext ctx) {
    container.stop();
    vertx.close(ctx.asyncAssertSuccess());
    client.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void simpleTest(TestContext should) {
    Async test = should.async();
    client.query("select * from arr_test", should.asyncAssertSuccess(res -> {
        should.assertEquals(1, res.getNumRows());
        JsonObject row = res.getRows().get(0);
        should.assertEquals(new JsonObject()
          .put("id", "1ff954bb-9808-4309-9955-fccf1a26266e")
          .put("value", new JsonArray().add(0.0d).add(1.0d)).encode(), row.encode());
        test.complete();
      }));
  }
}
