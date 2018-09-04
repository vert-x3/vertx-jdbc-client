package io.vertx.ext.jdbc;

import io.vertx.core.VertxOptions;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.core.metrics.impl.DummyVertxMetrics;
import io.vertx.core.spi.VertxMetricsFactory;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;
import io.vertx.ext.sql.SQLClient;
import io.vertx.test.core.VertxTestBase;
import io.vertx.test.fakemetrics.FakePoolMetrics;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class JDBCPoolMetricsTest extends VertxTestBase {

  private SQLClient client;
  private FakePoolMetrics metrics;

  public void after() throws Exception {
    if (client != null) {
      client.close();
    }
    super.after();
  }

  private SQLClient getClient() {
    if (client == null) {
      Map<String, PoolMetrics> metricsMap = FakePoolMetrics.getPoolMetrics();
      Set<String> keys = new HashSet<>(metricsMap.keySet());
      client = JDBCClient.createNonShared(vertx, JDBCClientTestBase.config().
          put("max_pool_size", 10));
      Set<String> after = new HashSet<>(metricsMap.keySet());
      after.removeAll(keys);
      metrics = (FakePoolMetrics) metricsMap.get(after.iterator().next());
    }
    return client;
  }

  @Override
  protected VertxOptions getOptions() {
    MetricsOptions options = new MetricsOptions().setEnabled(true);
    options.setFactory(new VertxMetricsFactory() {
      @Override
      public VertxMetrics metrics(VertxOptions options) {
        return new DummyVertxMetrics() {
          @Override
          public boolean isEnabled() {
            return true;
          }
          @Override
          public PoolMetrics<?> createPoolMetrics(String poolType, String poolName, int maxPoolSize) {
            if (poolType.equals("datasource")) {
              assertEquals("datasource", poolType);
              return new FakePoolMetrics(poolName, maxPoolSize);
            } else {
              return super.createPoolMetrics(poolType, poolName, maxPoolSize);
            }
          }
        };
      }
    });
    return new VertxOptions().setMetricsOptions(options);
  }

  @Test
  public void testLifecycle() {
    Map<String, PoolMetrics> metricsMap = FakePoolMetrics.getPoolMetrics();
    assertEquals(Collections.emptySet(), metricsMap.keySet());
    client = getClient();
    assertEquals(1, metricsMap.size());
    assertEquals(10, metrics.getPoolSize());
    client.close();
    client = null;
    assertEquals(0, metricsMap.size());
  }

  @Test
  public void testUseConnection() {
    client = getClient();
    client.getConnection(onSuccess(conn -> {
      assertEquals(0, metrics.numberOfWaitingTasks());
      assertEquals(1, metrics.numberOfRunningTasks());
      conn.close(onSuccess(v -> {
        assertEquals(0, metrics.numberOfWaitingTasks());
        assertEquals(0, metrics.numberOfRunningTasks());
        conn.close(ar -> {
          assertEquals(0, metrics.numberOfWaitingTasks());
          assertEquals(0, metrics.numberOfRunningTasks());
          testComplete();
        });
      }));
    }));
    await();
  }

  @Test
  public void testQueue() throws Exception {
    client = getClient();
    CompletableFuture<Void> close = new CompletableFuture<>();
    AtomicInteger closedCount = new AtomicInteger();
    CountDownLatch closedLatch = new CountDownLatch(11);
    for (int i = 0;i < 11;i++) {
      client.getConnection(onSuccess(conn -> {
        close.thenAccept(v -> {
          conn.close(ar -> {
            closedCount.decrementAndGet();
            closedLatch.countDown();
          });
        });
      }));
    }
    assertWaitUntil(() -> metrics.numberOfRunningTasks() == 10 && metrics.numberOfWaitingTasks() == 1);
    close.complete(null);
    awaitLatch(closedLatch);
    assertEquals(0, metrics.numberOfWaitingTasks());
    assertEquals(0, metrics.numberOfRunningTasks());
  }

}
