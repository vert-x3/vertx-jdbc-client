package io.vertx.jdbcclient;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;
import io.vertx.ext.jdbc.spi.impl.AgroalCPDataSourceProvider;
import io.vertx.ext.jdbc.spi.impl.C3P0DataSourceProvider;
import io.vertx.ext.jdbc.spi.impl.HikariCPDataSourceProvider;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.test.fakemetrics.FakePoolMetrics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class JDBCPoolProvidersTest extends ClientTestBase {

  private String poolMetricsPoolName;

  @Before
  public void setUp() throws Exception {
    poolMetricsPoolName = null;

    final MetricsOptions metricsOptions = new MetricsOptions().setEnabled(true);
    metricsOptions.setFactory(vertxOptions -> new VertxMetrics() {
      @Override
      public PoolMetrics<?> createPoolMetrics(String poolType, String poolName, int maxPoolSize) {
        if (poolType.equals("datasource")) {
          assertEquals("datasource", poolType);
          poolMetricsPoolName = poolName;
          return new FakePoolMetrics(poolName, maxPoolSize);
        } else {
          return VertxMetrics.super.createPoolMetrics(poolType, poolName, maxPoolSize);
        }
      }
    });

    final VertxOptions vertxOptions = new VertxOptions().setMetricsOptions(metricsOptions);
    vertx = Vertx.vertx(vertxOptions);
  }

  @Test
  public void testAgroal(TestContext should) {
    JDBCConnectOptions database = new JDBCConnectOptions().setJdbcUrl("jdbc:h2:mem:testDB?shutdown=true");
    PoolOptions poolOptions = new PoolOptions().setMaxSize(1).setName("customPoolName");
    client = JDBCPool.pool(vertx, database, poolOptions, new AgroalCPDataSourceProvider());
    simpleAssertSuccess(should)
      .onComplete(should.asyncAssertSuccess(v -> {
        should.assertEquals("customPoolName", poolMetricsPoolName);
    }));
  }

  @Test
  public void testHikari(TestContext should) {
    JDBCConnectOptions database = new JDBCConnectOptions().setJdbcUrl("jdbc:h2:mem:testDB?shutdown=true");
    PoolOptions poolOptions = new PoolOptions().setMaxSize(1).setName("customPoolName");
    client = JDBCPool.pool(vertx, database, poolOptions, new HikariCPDataSourceProvider());
    simpleAssertSuccess(should).onComplete(should.asyncAssertSuccess(v -> {
      should.assertEquals("customPoolName", poolMetricsPoolName);
    }));
  }

  @Test
  public void testC3P0(TestContext should) {
    JDBCConnectOptions database = new JDBCConnectOptions().setJdbcUrl("jdbc:h2:mem:testDB?shutdown=true");
    PoolOptions poolOptions = new PoolOptions().setMaxSize(1).setName("customPoolName");
    client = JDBCPool.pool(vertx, database, poolOptions, new C3P0DataSourceProvider());
    simpleAssertSuccess(should).onComplete(should.asyncAssertSuccess(v -> {
      should.assertEquals("customPoolName", poolMetricsPoolName);
    }));
  }

  private Future<Void> simpleAssertSuccess(TestContext should) {
    return client.query("SELECT * FROM INFORMATION_SCHEMA.TABLES")
          .execute()
          .onComplete(should.asyncAssertSuccess(rows -> should.assertTrue(rows.size() > 0)))
          .map((ignored) -> null);
  }
}
