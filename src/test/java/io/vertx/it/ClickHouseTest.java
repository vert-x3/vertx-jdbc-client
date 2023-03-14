package io.vertx.it;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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

@RunWith(VertxUnitRunner.class)
public class ClickHouseTest {

  private Vertx vertx;
  private ClickHouseContainer container;
  protected JDBCPool client;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    container = new ClickHouseContainer("yandex/clickhouse-server:20.8");
    container.withInitScript("init-clickhouse.sql");
    container.start();
    JsonObject config = new JsonObject()
      .put("driver_class", "ru.yandex.clickhouse.ClickHouseDriver")
      .put("url", "jdbc:clickhouse://localhost:" + container.getMappedPort(8123) + "/default");
    client = JDBCPool.pool(vertx, config);
  }

  @After
  public void after(TestContext should) {
    Async cleanup = should.async();
    client.close().onComplete(should.asyncAssertSuccess(res1 -> {
      vertx.close().onComplete(should.asyncAssertSuccess(res2 -> {
        container.close();
        cleanup.complete();
      }));
    }));
  }

  @Test
  public void simpleTest(TestContext should) {
    Async test = should.async();
    client.query("select * from arr_test")
      .execute().onComplete(should.asyncAssertSuccess(res -> {
        should.assertEquals(1, res.size());
        Row row = res.iterator().next();
        should.assertEquals(new JsonObject()
          .put("id", "1ff954bb-9808-4309-9955-fccf1a26266e")
          .put("value", new JsonArray().add(0.0d).add(1.0d)), row.toJson());
        test.complete();
      }));
  }
}
