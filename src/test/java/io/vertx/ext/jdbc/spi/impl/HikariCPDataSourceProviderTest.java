package io.vertx.ext.jdbc.spi.impl;

import io.vertx.core.json.JsonObject;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Checks the behavior of {@link HikariCPDataSourceProvider}.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class HikariCPDataSourceProviderTest {


  @Test
  public void testCreationOfTheHikariCPDataSource() throws SQLException {
    HikariCPDataSourceProvider provider = new HikariCPDataSourceProvider();

    JsonObject configuration = new JsonObject();
    configuration
        .put("foo", "bar")
        .put("jdbcUrl", "jdbc:hsqldb:mem:test?shutdown=true")
        .put("maxLifetime", 200L)
        .put("maximumPoolSize", 30);

    DataSource dataSource = provider.getDataSource(configuration);
    assertNotNull(dataSource);
    assertNotNull(dataSource.getConnection());

    assertEquals(30, provider.maximumPoolSize(dataSource, configuration));

    provider.close(dataSource);
  }

  @Test
  public void testCreationOfTheHikariCPDataSourceWithInteger() throws SQLException {
    HikariCPDataSourceProvider provider = new HikariCPDataSourceProvider();

    JsonObject configuration = new JsonObject();
    configuration
        .put("foo", "bar")
        .put("jdbcUrl", "jdbc:hsqldb:mem:test?shutdown=true")
        .put("maxLifetime", 200);

    DataSource dataSource = provider.getDataSource(configuration);
    assertNotNull(dataSource);
    assertNotNull(dataSource.getConnection());

    provider.close(dataSource);
  }


  @Test
  public void testLeakDetectionOfTheHikariCPDataSourceWithLong() throws SQLException {
    HikariCPDataSourceProvider provider = new HikariCPDataSourceProvider();

    JsonObject configuration = new JsonObject();
    configuration
            .put("foo", "bar")
            .put("jdbcUrl", "jdbc:hsqldb:mem:test?shutdown=true")
            .put("leakDetectionThreshold", 10000);

    DataSource dataSource = provider.getDataSource(configuration);
    assertNotNull(dataSource);
    assertNotNull(dataSource.getConnection());

    provider.close(dataSource);
  }

}
