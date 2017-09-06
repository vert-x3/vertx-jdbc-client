package io.vertx.ext.jdbc.spi.impl;

import io.vertx.core.json.JsonObject;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Checks the behavior of {@link AgroalCPDataSourceProvider}.
 */
public class AgroalCPDataSourceProviderTest {

  @Test
  public void testCreationOfTheAgroalDataSource() throws SQLException {
    AgroalCPDataSourceProvider provider = new AgroalCPDataSourceProvider();

    JsonObject configuration = new JsonObject();
    configuration
      .put("jdbcUrl", "jdbc:h2:mem:test?shutdown=true")
      .put("driverClassName", "org.h2.Driver")
      .put("minSize", 1)
      .put("maxSize", 30)
      .put("principal", "")
      .put("credential", "");

    DataSource dataSource = provider.getDataSource(configuration);
    assertNotNull(dataSource);
    assertNotNull(dataSource.getConnection());

    assertEquals(30, provider.maximumPoolSize(dataSource, configuration));

    provider.close(dataSource);
  }
}
