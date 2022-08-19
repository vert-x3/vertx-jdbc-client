package io.vertx.jdbcclient;

import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.Future;
import io.vertx.core.VertxOptions;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.test.fakemetrics.FakePoolMetrics;
import org.h2.Driver;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.DataSourceProvider;
import io.vertx.ext.jdbc.spi.impl.AgroalCPDataSourceProvider;
import io.vertx.ext.jdbc.spi.impl.C3P0DataSourceProvider;
import io.vertx.ext.jdbc.spi.impl.HikariCPDataSourceProvider;
import io.vertx.ext.unit.TestContext;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class JDBCPoolInitTest extends ClientTestBase {

  private volatile String poolMetricsPoolName;

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
  public void test_init_pool_by_Hikari_provider(TestContext should) {
    final JsonObject config = new JsonObject().put("provider_class", HikariCPDataSourceProvider.class.getName())
                                              .put("jdbcUrl", "jdbc:h2:mem:testDB?shutdown=true");
    client = JDBCPool.pool(vertx, DataSourceProvider.create(config));
    simpleAssertSuccess(should);
  }

  @Test
  public void test_init_pool_by_Hikari_config(TestContext should) {
    final JsonObject config = new JsonObject().put("provider_class", HikariCPDataSourceProvider.class.getName())
                                              .put("jdbcUrl", "jdbc:h2:mem:testDB?shutdown=true")
                                              .put("driverClassName", Driver.class.getName());
    client = JDBCPool.pool(vertx, config);
    simpleAssertSuccess(should);
  }

  @Test
  public void test_init_pool_by_C3P0_config(TestContext should) {
    final JsonObject config = new JsonObject().put("provider_class", C3P0DataSourceProvider.class.getName())
                                              .put("url", "jdbc:h2:mem:testDB?shutdown=true");
    client = JDBCPool.pool(vertx, config);
    simpleAssertSuccess(should);
  }

  @Test
  public void test_init_pool_by_C3P0_provider(TestContext should) {
    final JsonObject config = new JsonObject().put("provider_class", C3P0DataSourceProvider.class.getName())
                                              .put("url", "jdbc:h2:mem:testDB?shutdown=true")
                                              .put("driver_class", Driver.class.getName());
    client = JDBCPool.pool(vertx, DataSourceProvider.create(config));
    simpleAssertSuccess(should);
  }

  @Test
  public void test_init_pool_by_Agroal_provider(TestContext should) {
    final JsonObject config = new JsonObject().put("provider_class", AgroalCPDataSourceProvider.class.getName())
                                              .put("jdbcUrl", "jdbc:h2:mem:testDB?shutdown=true")
                                              .put("principal", "")
                                              .put("credential", "");
    client = JDBCPool.pool(vertx, DataSourceProvider.create(config));
    simpleAssertSuccess(should);
  }

  @Test
  public void test_init_pool_by_Agroal_config(TestContext should) {
    final JsonObject config = new JsonObject().put("provider_class", AgroalCPDataSourceProvider.class.getName())
                                              .put("jdbcUrl", "jdbc:h2:mem:testDB?shutdown=true")
                                              .put("driverClassName", Driver.class.getName())
                                              .put("principal", "")
                                              .put("credential", "");
    client = JDBCPool.pool(vertx, DataSourceProvider.create(config));
    simpleAssertSuccess(should);
  }

  private Future<Void> simpleAssertSuccess(TestContext should) {
    return client.query("SELECT * FROM INFORMATION_SCHEMA.TABLES")
          .execute()
          .onComplete(should.asyncAssertSuccess(rows -> should.assertTrue(rows.size() > 0)))
          .map((ignored) -> null);
  }

  @Test
  public void test_init_pool_by_existing_datasource(TestContext should) {
    HikariDataSource ds = new HikariDataSource();
    ds.setJdbcUrl("jdbc:h2:mem:testDB?shutdown=true");
    client = JDBCPool.pool(vertx, ds);
    simpleAssertSuccess(should);
  }

  @Test
  public void test_init_pool_by_existing_datasource_custom_config(TestContext should) {
    final JsonObject config = new JsonObject()
      .put("url", "jdbc:h2:mem:testDB?shutdown=true")
      .put("user", "")
      .put("database", "testDB")
      .put("maxPoolSize", 10);

    HikariDataSource ds = new HikariDataSource();
    ds.setJdbcUrl("jdbc:h2:mem:testDB?shutdown=true");
    client = JDBCPool.pool(vertx, ds, config);
    simpleAssertSuccess(should);
  }

  @Test
  public void test_init_pool_with_datasource_name(TestContext should) {
    final String datasourceName = "customDatasourceName";

    final JsonObject config = new JsonObject()
      .put("url", "jdbc:h2:mem:testDB?shutdown=true")
      .put("datasourceName", datasourceName);

    client = JDBCPool.pool(vertx, config);
    simpleAssertSuccess(should)
      .onComplete(should.asyncAssertSuccess(nil -> {
        should.assertEquals(datasourceName, poolMetricsPoolName);
      }));
  }

  @Test
  public void test_init_pool_without_datasource_name_uses_uuid_as_datasource_name(TestContext should) {
    final JsonObject config = new JsonObject()
      .put("url", "jdbc:h2:mem:testDB?shutdown=true");

    client = JDBCPool.pool(vertx, config);
    simpleAssertSuccess(should)
      .onComplete(should.asyncAssertSuccess(nil -> {
        should.assertTrue(poolMetricsPoolName.matches("^[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}$"));
      }));
  }

  @Test
  public void test_init_pool_with_jdbc_connect_options_and_pool_options_uses_pool_name_as_datasource_name(TestContext should) {
    final String poolName = "customPoolName";

    client = JDBCPool.pool(
      vertx,
      new JDBCConnectOptions().setJdbcUrl("jdbc:h2:mem:testDB?shutdown=true"),
      new PoolOptions().setName(poolName)
    );

    simpleAssertSuccess(should)
      .onComplete(should.asyncAssertSuccess(nil -> {
        should.assertEquals(poolName, poolMetricsPoolName);
      }));
  }

  @Test
  public void test_init_pool_with_datasource_provider_uses_datasource_name_from_provider_config(TestContext should) {
    final String datasourceName = "customDatasourceName";

    final JsonObject config = new JsonObject()
      .put("provider_class", AgroalCPDataSourceProvider.class.getName())
      .put("principal", "")
      .put("credential", "")
      .put("jdbcUrl", "jdbc:h2:mem:testDB?shutdown=true")
      .put("datasourceName", datasourceName);

    client = JDBCPool.pool(vertx, DataSourceProvider.create(config));

    simpleAssertSuccess(should)
      .onComplete(should.asyncAssertSuccess(nil -> {
        should.assertEquals(datasourceName, poolMetricsPoolName);
      }));
  }
}
