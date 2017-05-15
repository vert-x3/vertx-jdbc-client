package io.vertx.ext.jdbc;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.DataSourceProvider;
import io.vertx.ext.jdbc.spi.impl.C3P0DataSourceProvider;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.*;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class CloseTest extends JDBCClientTestBase {

  private static final JsonObject theConfig = config();

  public static class NonSharedClientVerticle extends AbstractVerticle {
    @Override
    public void start(io.vertx.core.Future<Void> f) throws Exception {
      SQLClient client = JDBCClient.createNonShared(vertx, theConfig);
      String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
      client.getConnection(ar1 -> {
        if (ar1.succeeded()) {
          SQLConnection conn = ar1.result();
          conn.query(sql, ar2 -> {
            if (ar2.succeeded()) {
              f.complete();
            } else {
              f.fail(ar2.cause());
            }
          });
        } else {
          f.fail(ar1.cause());
        }
      });
    }
  }

  @Test
  public void testUsingNonSharedInVerticle() throws Exception {
    CompletableFuture<String> id = new CompletableFuture<>();
    vertx.deployVerticle(NonSharedClientVerticle.class.getName(), onSuccess(id::complete));
    close(id.get(10, TimeUnit.SECONDS), false);
  }

  @Test
  public void testUsingNonSharedInVerticle2() throws Exception {
    CompletableFuture<String> id = new CompletableFuture<>();
    vertx.deployVerticle(NonSharedClientVerticle.class.getName(), new DeploymentOptions().setInstances(2), onSuccess(id::complete));
    close(id.get(10, TimeUnit.SECONDS), false);
  }

  public static class SharedClientVerticle extends AbstractVerticle {
    @Override
    public void start(io.vertx.core.Future<Void> f) throws Exception {
      SQLClient client = JDBCClient.createShared(vertx, theConfig);
      String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
      client.getConnection(ar1 -> {
        if (ar1.succeeded()) {
          SQLConnection conn = ar1.result();
          conn.query(sql, ar2 -> {
            if (ar2.succeeded()) {
              f.complete();
            } else {
              f.fail(ar2.cause());
            }
          });
        } else {
          f.fail(ar1.cause());
        }
      });
    }
  }

  @Test
  public void testUsingSharedInVerticle() throws Exception {
    CompletableFuture<String> id = new CompletableFuture<>();
    vertx.deployVerticle(SharedClientVerticle.class.getName(), new DeploymentOptions().setInstances(1), onSuccess(id::complete));
    close(id.get(10, TimeUnit.SECONDS), false);
  }

  @Test
  public void testUsingSharedInVerticle2() throws Exception {
    CompletableFuture<String> id = new CompletableFuture<>();
    vertx.deployVerticle(SharedClientVerticle.class.getName(), new DeploymentOptions().setInstances(2), onSuccess(id::complete));
    close(id.get(10, TimeUnit.SECONDS), false);
  }

  private static DataSource ds;

  public static class ProvidedDataSourceVerticle extends AbstractVerticle {
    @Override
    public void start(io.vertx.core.Future<Void> f) throws Exception {
      SQLClient client = JDBCClient.create(vertx, ds);
      String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
      client.getConnection(ar1 -> {
        if (ar1.succeeded()) {
          SQLConnection conn = ar1.result();
          conn.query(sql, ar2 -> {
            if (ar2.succeeded()) {
              f.complete();
            } else {
              f.fail(ar2.cause());
            }
          });
        } else {
          f.fail(ar1.cause());
        }
      });
    }
  }

  @Test
  public void testUsingProvidedDataSourceVerticle() throws Exception {
    DataSourceProvider provider = new C3P0DataSourceProvider();
    ds = provider.getDataSource(theConfig);
    CompletableFuture<String> id = new CompletableFuture<>();
    vertx.deployVerticle(ProvidedDataSourceVerticle.class.getName(), new DeploymentOptions().setInstances(1), onSuccess(id::complete));
    close(id.get(10, TimeUnit.SECONDS), true);
  }

  private void close(String deploymentId, boolean expectedDsThreadStatus) throws Exception {
    List<Thread> getConnThread = findThreads(t -> t.getName().equals("vertx-jdbc-service-get-connection-thread"));
    assertTrue(getConnThread.size() > 0);
    List<Thread> poolThreads = findThreads(t -> t.getName().startsWith("C3P0PooledConnectionPoolManager"));
    assertTrue(poolThreads.size() > 0);
    CountDownLatch closeLatch = new CountDownLatch(1);
    vertx.undeploy(deploymentId, onSuccess(v -> {
      closeLatch.countDown();
    }));
    awaitLatch(closeLatch);
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 5000) {
      if (!getConnThread.get(0).isAlive() && poolThreads.stream().allMatch(t -> t.isAlive() == expectedDsThreadStatus)) {
        return;
      }
      MILLISECONDS.sleep(10);
    }
    fail("Timeout waiting for connection threads to be dead");
  }

  private List<Thread> findThreads(Predicate<Thread> predicate) {
    return Thread.getAllStackTraces()
        .keySet()
        .stream()
        .filter(predicate)
        .collect(Collectors.toList());
  }
}
