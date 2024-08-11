package io.vertx.ext.jdbc.spi.impl;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.DataSourceProvider;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.sqlclient.PoolOptions;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public class AgroalCPDataSourceProvider implements DataSourceProvider {

  public static final String NAME = "Agroal";

  private JsonObject initConfig;

  @Override
  public DataSourceProvider init(JsonObject sqlConfig) {
    this.initConfig = sqlConfig;
    return this;
  }

  @Override
  public JsonObject getInitialConfig() {
    return Optional.ofNullable(initConfig).orElseGet(DataSourceProvider.super::getInitialConfig);
  }

  @Override
  public int maximumPoolSize(DataSource dataSource, JsonObject config) throws SQLException {
    if (dataSource instanceof AgroalDataSource) {
      return ((AgroalDataSource) dataSource).getConfiguration().connectionPoolConfiguration().maxSize();
    }
    return -1;
  }

  @Override
  public JsonObject toJson(JDBCConnectOptions connectOptions, PoolOptions poolOptions) {
    JsonObject config = new JsonObject();
    config.put("dataSourceImplementation", connectOptions.getDataSourceImplementation());
    config.put("connectionValidationTimeout", connectOptions.getConnectTimeout());
    config.put("minSize", 0);
    config.put("initialSize", 1);
    config.put("maxSize", poolOptions.getMaxSize());
    config.put("acquisitionTimeout", connectOptions.getConnectTimeout());
    config.put("connectionReapTimeout", connectOptions.getIdleTimeout());
    config.put("connectionLeakTimeout", connectOptions.getIdleTimeout());
    config.put("jdbcUrl", connectOptions.getJdbcUrl());
    if (connectOptions.getUser() != null) {
      config.put("principal", connectOptions.getUser());
    }
    if (connectOptions.getPassword() != null) {
      config.put("credential", connectOptions.getPassword());
    }
    return config;
  }

  @Override
  public DataSource getDataSource(JsonObject cfg) throws SQLException {
    JsonObject config = cfg == null || cfg.isEmpty() ? initConfig : cfg;
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
        .connectionFactoryConfiguration(cf -> {
            if (config.getString("principal") != null) {
              cf = cf.principal(new NamePrincipal(config.getString("principal")));
            }
            if (config.getString("credential") != null) {
              cf = cf.credential(new SimplePassword(config.getString("credential")));
            }
            return cf.jdbcUrl(config.getString("jdbcUrl"));
          }
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
