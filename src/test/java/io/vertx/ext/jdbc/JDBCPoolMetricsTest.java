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

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class JDBCPoolMetricsTest extends VertxTestBase {

  private String dataSourceName = UUID.randomUUID().toString();
  private SQLClient client;

  public void after() throws Exception {
    if (client != null) {
      client.close();
    }
    super.after();
  }

  private SQLClient getClient() {
    if (client == null) {
      client = JDBCClient.createShared(vertx, JDBCClientTestBase.config().put("max_pool_size", 10), dataSourceName);
    }
    return client;
  }

  private FakePoolMetrics getMetrics() {
    return (FakePoolMetrics) FakePoolMetrics.getPoolMetrics().get(dataSourceName);
  }

  @Override
  protected VertxOptions getOptions() {
    MetricsOptions options = new MetricsOptions().setEnabled(true);
    options.setFactory(new VertxMetricsFactory() {
      @Override
      public VertxMetrics metrics(VertxOptions options) {
        return new DummyVertxMetrics() {
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
    assertEquals(0, metricsMap.size());
    client.getConnection(onSuccess(conn -> {
      assertEquals(1, metricsMap.size());
      assertEquals(10, getMetrics().getPoolSize());
      conn.close(onSuccess(connClosed -> {
        client.close(onSuccess(clientClose -> {
          client = null;
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
      assertEquals(0, getMetrics().numberOfWaitingTasks());
      assertEquals(1, getMetrics().numberOfRunningTasks());
      conn.close(onSuccess(v -> {
        assertEquals(0, getMetrics().numberOfWaitingTasks());
        assertEquals(0, getMetrics().numberOfRunningTasks());
        conn.close(ar -> {
          assertEquals(0, getMetrics().numberOfWaitingTasks());
          assertEquals(0, getMetrics().numberOfRunningTasks());
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
    assertEquals(10, getMetrics().numberOfRunningTasks());
    assertEquals(1, getMetrics().numberOfWaitingTasks());
    close.complete(null);
    awaitLatch(closedLatch);
    assertEquals(0, getMetrics().numberOfWaitingTasks());
    assertEquals(0, getMetrics().numberOfRunningTasks());
  }

}
