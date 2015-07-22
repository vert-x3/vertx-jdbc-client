package io.vertx.ext.jdbc.spi.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

public class HikariCPDataSourceProvider implements DataSourceProvider {

  @Override
  public DataSource getDataSource(JsonObject json) throws SQLException {

    final HikariConfig config = new HikariConfig();

    for (Map.Entry<String, Object> entry : json) {
      switch (entry.getKey()) {
        case "dataSourceClassName":
          config.setDataSourceClassName((String) entry.getValue());
          break;
        case "jdbcUrl":
          config.setJdbcUrl((String) entry.getValue());
          break;
        case "username":
          config.setUsername((String) entry.getValue());
          break;
        case "password":
          config.setPassword((String) entry.getValue());
          break;
        case "autoCommit":
          config.setAutoCommit((Boolean) entry.getValue());
          break;
        case "connectionTimeout":
          config.setConnectionTimeout((Long) entry.getValue());
          break;
        case "idleTimeout":
          config.setIdleTimeout((Long) entry.getValue());
          break;
        case "maxLifetime":
          config.setMaxLifetime((Long) entry.getValue());
          break;
        case "connectionTestQuery":
          config.setConnectionTestQuery((String) entry.getValue());
          break;
        case "minimumIdle":
          config.setMinimumIdle((Integer) entry.getValue());
          break;
        case "maximumPoolSize":
          config.setMaximumPoolSize((Integer) entry.getValue());
          break;
        case "metricRegistry":
          throw new UnsupportedOperationException(entry.getKey());
        case "healthCheckRegistry":
          throw new UnsupportedOperationException(entry.getKey());
        case "poolName":
          config.setPoolName((String) entry.getValue());
          break;
        case "initializationFailFast":
          config.setInitializationFailFast((Boolean) entry.getValue());
          break;
        case "isolationInternalQueries":
          config.setIsolateInternalQueries((Boolean) entry.getValue());
          break;
        case "allowPoolSuspension":
          config.setAllowPoolSuspension((Boolean) entry.getValue());
          break;
        case "readOnly":
          config.setReadOnly((Boolean) entry.getValue());
          break;
        case "registerMBeans":
          config.setRegisterMbeans((Boolean) entry.getValue());
          break;
        case "catalog":
          config.setCatalog((String) entry.getValue());
          break;
        case "connectionInitSql":
          config.setConnectionInitSql((String) entry.getValue());
          break;
        case "driverClassName":
          config.setDriverClassName((String) entry.getValue());
          break;
        case "transactionIsolation":
          config.setTransactionIsolation((String) entry.getValue());
          break;
        case "validationTimeout":
          config.setValidationTimeout((Long) entry.getValue());
          break;
        case "leakDetectionThreshold":
          config.setLeakDetectionThreshold((Long) entry.getValue());
          break;
        case "dataSource":
          throw new UnsupportedOperationException(entry.getKey());
        case "threadFactory":
          throw new UnsupportedOperationException(entry.getKey());
      }
    }

    return new HikariDataSource(config);
  }

  @Override
  public void close(DataSource dataSource) throws SQLException {
    if (dataSource instanceof HikariDataSource) {
      ((HikariDataSource) dataSource).close();
    }
  }
}
