package io.vertx.ext.jdbc;

import io.vertx.core.VertxOptions;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.core.spi.VertxMetricsFactory;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;
import io.vertx.ext.sql.SQLClient;
import io.vertx.test.fakemetrics.FakePoolMetrics;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class JDBCPoolMetricsTest extends JDBCClientTestBase {

  private String dataSourceName = UUID.randomUUID().toString();

  private SQLClient getClient() {
    if (client == null) {
      client = JDBCClient.createShared(vertx, DBConfigs.hsqldb(JDBCPoolMetricsTest.class).put("max_pool_size", 10), dataSourceName);
    }
    return client;
  }

//  protected FakePoolMetrics getMetrics() {
//    return (FakePoolMetrics) FakePoolMetrics.getPoolMetrics().get(dataSourceName);
//  }

  protected FakePoolMetrics fakeMetrics() {
    return (FakePoolMetrics) FakePoolMetrics.getPoolMetrics().get(dataSourceName);
  }

  @Override
  protected VertxOptions getOptions() {
    MetricsOptions options = new MetricsOptions().setEnabled(true);
    return new VertxOptions().setMetricsOptions(options);
  }

  @Override
  protected VertxMetricsFactory getMetrics() {
    return o -> new VertxMetrics() {
      @Override
      public PoolMetrics<?> createPoolMetrics(String poolType, String poolName, int maxPoolSize) {
        if (poolType.equals("datasource")) {
          assertEquals("datasource", poolType);
          return new FakePoolMetrics(poolName, maxPoolSize);
        } else {
          return VertxMetrics.super.createPoolMetrics(poolType, poolName, maxPoolSize);
        }
      }
    };
  }

  @Test
  public void testLifecycle() {
    Map<String, PoolMetrics> metricsMap = FakePoolMetrics.getPoolMetrics();
    assertEquals(Collections.emptySet(), metricsMap.keySet());
    client = getClient();
    assertEquals(0, metricsMap.size());
    client.getConnection(onSuccess(conn -> {
      assertEquals(1, metricsMap.size());
      assertEquals(10, fakeMetrics().getPoolSize());
      conn.close(onSuccess(connClosed -> {
        client.close(onSuccess(clientClose -> {
          assertEquals(0, metricsMap.size());
          testComplete();
        }));
      }));
    }));
    await();
  }

  @Test
  public void testUseConnection() {
    client = getClient();
    client.getConnection(onSuccess(conn -> {
      assertEquals(0, fakeMetrics().numberOfWaitingTasks());
      assertEquals(1, fakeMetrics().numberOfRunningTasks());
      conn.close(onSuccess(v -> {
        assertEquals(0, fakeMetrics().numberOfWaitingTasks());
        assertEquals(0, fakeMetrics().numberOfRunningTasks());
        conn.close(ar -> {
          assertEquals(0, fakeMetrics().numberOfWaitingTasks());
          assertEquals(0, fakeMetrics().numberOfRunningTasks());
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
    CountDownLatch connectedLatch = new CountDownLatch(10);
    for (int i = 0; i < 11; i++) {
      client.getConnection(onSuccess(conn -> {
        connectedLatch.countDown();
        close.thenAccept(v -> {
          conn.close(ar -> {
            closedCount.decrementAndGet();
            closedLatch.countDown();
          });
        });
      }));
    }
    awaitLatch(connectedLatch);
    assertEquals(10, fakeMetrics().numberOfRunningTasks());
    assertEquals(1, fakeMetrics().numberOfWaitingTasks());
    close.complete(null);
    awaitLatch(closedLatch);
    assertEquals(0, fakeMetrics().numberOfWaitingTasks());
    assertEquals(0, fakeMetrics().numberOfRunningTasks());
  }

}
