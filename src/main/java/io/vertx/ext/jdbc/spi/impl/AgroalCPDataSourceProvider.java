package io.vertx.ext.jdbc.spi.impl;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public class AgroalCPDataSourceProvider implements DataSourceProvider {
  @Override
  public int maximumPoolSize(DataSource dataSource, JsonObject config) throws SQLException {
    if (dataSource instanceof AgroalDataSource) {
      return ((AgroalDataSource) dataSource).getConfiguration().connectionPoolConfiguration().maxSize();
    }
    return -1;
  }

  @Override
  public DataSource getDataSource(JsonObject config) throws SQLException {

    AgroalDataSourceConfigurationSupplier dataSourceConfigurationBuilder = new AgroalDataSourceConfigurationSupplier()
      .dataSourceImplementation(AgroalDataSourceConfiguration.DataSourceImplementation.valueOf(config.getString("dataSourceImplementation", "AGROAL")))
      .metricsEnabled(config.getBoolean("metricsEnabled", false))
      .connectionPoolConfiguration( cp -> cp
        .validationTimeout(Duration.ofMillis(config.getLong("connectionValidationTimeout", 30_000L)))
        .minSize(config.getInteger("minSize", 0))
        .maxSize(config.getInteger("maxSize", 30))
        .initialSize(config.getInteger("initialSize", 1))
        .acquisitionTimeout(Duration.ofMillis(config.getInteger("acquisitionTimeout", 0)))
        .reapTimeout(Duration.ofMillis(config.getLong("connectionReapTimeout", 0L)))
        .leakTimeout(Duration.ofMillis(config.getLong("connectionLeakTimeout", 0L)))
        .connectionFactoryConfiguration( cf -> cf
          .driverClassName(config.getString("driverClassName"))
          .jdbcUrl(config.getString("jdbcUrl"))
          .principal(new NamePrincipal(config.getString("principal")))
          .credential(new SimplePassword(config.getString("credential")))
        )
      );

    return AgroalDataSource.from(dataSourceConfigurationBuilder);
  }

  @Override
  public void close(DataSource dataSource) throws SQLException {
    if (dataSource instanceof AgroalDataSource) {
      ((AgroalDataSource) dataSource).close();
    }
  }
}
