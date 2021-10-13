package io.vertx.jdbcclient;

import com.zaxxer.hikari.HikariDataSource;
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

import javax.sql.DataSource;

public class JDBCPoolInitTest extends ClientTestBase {

  @Before
  public void setUp() throws Exception {
    vertx = Vertx.vertx();
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

  private void simpleAssertSuccess(TestContext should) {
    client.query("SELECT * FROM INFORMATION_SCHEMA.TABLES")
          .execute()
          .onComplete(should.asyncAssertSuccess(rows -> should.assertTrue(rows.size() > 0)));
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
}
