package io.vertx.ext.jdbc;

import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.sql.SQLClient;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class RefCountTest extends VertxTestBase {

  private LocalMap<String, Object> getLocalMap() {
    return vertx.sharedData().getLocalMap("__vertx.JDBCClient.datasources");
  }

  @Test
  public void testNonShared() {
    LocalMap<String, Object> map = getLocalMap();
    JsonObject config = new JsonObject();
    config.put("provider_class", TestDSProvider.class.getName());
    SQLClient client1 = JDBCClient.createNonShared(vertx, config);
    assertEquals(1, TestDSProvider.instanceCount.get());
    SQLClient client2 = JDBCClient.createNonShared(vertx, config);
    assertEquals(2, TestDSProvider.instanceCount.get());
    SQLClient client3 = JDBCClient.createNonShared(vertx, config);
    assertEquals(3, TestDSProvider.instanceCount.get());
    client1.close();
    assertWaitUntil(() -> TestDSProvider.instanceCount.get() == 2);
    client2.close();
    assertWaitUntil(() -> TestDSProvider.instanceCount.get() == 1);
    client3.close();
    assertWaitUntil(() -> TestDSProvider.instanceCount.get() == 0);
    assertWaitUntil(() -> getLocalMap().size() == 0);
    assertWaitUntil(() -> map != getLocalMap()); // Map has been closed
  }

  @Test
  public void testSharedDefault() throws Exception {
    LocalMap<String, Object> map = getLocalMap();
    JsonObject config = new JsonObject();
    config.put("provider_class", TestDSProvider.class.getName());
    SQLClient client1 = JDBCClient.createShared(vertx, config);
    assertEquals(1, TestDSProvider.instanceCount.get());
    assertEquals(1, map.size());
    SQLClient client2 = JDBCClient.createShared(vertx, config);
    assertEquals(1, TestDSProvider.instanceCount.get());
    assertEquals(1, map.size());
    SQLClient client3 = JDBCClient.createShared(vertx, config);
    assertEquals(1, TestDSProvider.instanceCount.get());
    assertEquals(1, map.size());
    client1.close();
    Thread.sleep(200);
    assertEquals(1, TestDSProvider.instanceCount.get());
    assertEquals(1, map.size());
    client2.close();
    assertEquals(1, TestDSProvider.instanceCount.get());
    assertEquals(1, map.size());
    client3.close();
    assertWaitUntil(() -> TestDSProvider.instanceCount.get() == 0);
    assertWaitUntil(() -> map.size() == 0);
    assertWaitUntil(() -> map != getLocalMap()); // Map has been closed
  }

  @Test
  public void testSharedNamed() throws Exception {
    LocalMap<String, Object> map = getLocalMap();
    JsonObject config = new JsonObject();
    config.put("provider_class", TestDSProvider.class.getName());
    SQLClient client1 = JDBCClient.createShared(vertx, config, "ds1");
    assertEquals(1, TestDSProvider.instanceCount.get());
    assertEquals(1, map.size());
    SQLClient client2 = JDBCClient.createShared(vertx, config, "ds1");
    assertEquals(1, TestDSProvider.instanceCount.get());
    assertEquals(1, map.size());
    SQLClient client3 = JDBCClient.createShared(vertx, config, "ds1");
    assertEquals(1, TestDSProvider.instanceCount.get());
    assertEquals(1, map.size());

    SQLClient client4 = JDBCClient.createShared(vertx, config, "ds2");
    assertEquals(2, TestDSProvider.instanceCount.get());
    assertEquals(2, map.size());
    SQLClient client5 = JDBCClient.createShared(vertx, config, "ds2");
    assertEquals(2, TestDSProvider.instanceCount.get());
    assertEquals(2, map.size());
    SQLClient client6 = JDBCClient.createShared(vertx, config, "ds2");
    assertEquals(2, TestDSProvider.instanceCount.get());
    assertEquals(2, map.size());

    client1.close();
    Thread.sleep(200);
    assertEquals(2, TestDSProvider.instanceCount.get());
    assertEquals(2, map.size());
    client2.close();
    assertEquals(2, TestDSProvider.instanceCount.get());
    assertEquals(2, map.size());
    client3.close();
    assertWaitUntil(() -> TestDSProvider.instanceCount.get() == 1);
    assertWaitUntil(() -> map.size() == 1);

    client4.close();
    Thread.sleep(200);
    assertEquals(1, TestDSProvider.instanceCount.get());
    assertEquals(1, map.size());
    client5.close();
    assertEquals(1, TestDSProvider.instanceCount.get());
    assertEquals(1, map.size());
    client6.close();
    assertWaitUntil(() -> TestDSProvider.instanceCount.get() == 0);
    assertWaitUntil(() -> map.size() == 0);
    assertWaitUntil(() -> map != getLocalMap()); // Map has been closed
  }
}
