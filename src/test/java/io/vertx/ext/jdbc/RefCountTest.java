package io.vertx.ext.jdbc;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.sql.SQLClient;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class RefCountTest extends VertxTestBase {

  private LocalMap<String, Object> map;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    map = vertx.sharedData().getLocalMap("__vertx.JDBCClient.datasources");
  }

  @Test
  public void testNonShared() {
    JsonObject config = new JsonObject();
    SQLClient client1 = JDBCClient.create(vertx, config);
    assertEquals(1, map.size());
    SQLClient client2 = JDBCClient.create(vertx, config);
    assertEquals(2, map.size());
    SQLClient client3 = JDBCClient.create(vertx, config);
    assertEquals(3, map.size());

    close(client1, 2)
      .compose(v -> close(client2, 1))
      .compose(v -> close(client3, 0))
      .setHandler(onSuccess(v -> testComplete()));

    await();
  }

  @Test
  public void testSharedDefault() throws Exception {
    JsonObject config = new JsonObject();
    SQLClient client1 = JDBCClient.createShared(vertx, config);
    assertEquals(1, map.size());
    SQLClient client2 = JDBCClient.createShared(vertx, config);
    assertEquals(1, map.size());
    SQLClient client3 = JDBCClient.createShared(vertx, config);
    assertEquals(1, map.size());

    close(client1, 1)
      .compose(v -> close(client2, 1))
      .compose(v -> close(client3, 0))
      .setHandler(onSuccess(v -> testComplete()));

    await();
  }

  @Test
  public void testSharedNamed() throws Exception {
    JsonObject config = new JsonObject();
    SQLClient client1 = JDBCClient.createShared(vertx, config, "ds1");
    assertEquals(1, map.size());
    SQLClient client2 = JDBCClient.createShared(vertx, config, "ds1");
    assertEquals(1, map.size());
    SQLClient client3 = JDBCClient.createShared(vertx, config, "ds1");
    assertEquals(1, map.size());

    SQLClient client4 = JDBCClient.createShared(vertx, config, "ds2");
    assertEquals(2, map.size());
    SQLClient client5 = JDBCClient.createShared(vertx, config, "ds2");
    assertEquals(2, map.size());
    SQLClient client6 = JDBCClient.createShared(vertx, config, "ds2");
    assertEquals(2, map.size());

    close(client1, 2)
      .compose(v -> close(client2, 2))
      .compose(v -> close(client3, 1))
      .compose(v -> close(client4, 1))
      .compose(v -> close(client5, 1))
      .compose(v -> close(client6, 0))
      .setHandler(onSuccess(v -> testComplete()));

    await();
  }

  private Future<Void> close(SQLClient client, int expectedMapSize) {
    Promise<Void> promise = Promise.promise();
    client.close(promise);
    return promise.future().compose(v -> {
      assertEquals(expectedMapSize, map.size());
      return Future.succeededFuture(v);
    });
  }
}
