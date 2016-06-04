/*
* Copyright (c) 2011-2015 The original author or authors
* ------------------------------------------------------
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Eclipse Public License v1.0
* and Apache License v2.0 which accompanies this distribution.
*
*     The Eclipse Public License is available at
*     http://www.eclipse.org/legal/epl-v10.html
*
*     The Apache License v2.0 is available at
*     http://www.opensource.org/licenses/apache2.0.php
*
* You may elect to redistribute this code under either of these licenses.
*/
package io.vertx.ext.jdbc.spi.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
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
          config.setConnectionTimeout(getLong(entry.getValue()));
          break;
        case "idleTimeout":
          config.setIdleTimeout(getLong(entry.getValue()));
          break;
        case "maxLifetime":
          config.setMaxLifetime(getLong(entry.getValue()));
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
          config.setValidationTimeout(getLong(entry.getValue()));
          break;
        case "leakDetectionThreshold":
          config.setLeakDetectionThreshold(getLong(entry.getValue()));
          break;
        case "dataSource":
          throw new UnsupportedOperationException(entry.getKey());
        case "threadFactory":
          throw new UnsupportedOperationException(entry.getKey());
        case "datasource":
          // extension to support configuring datasource.* properties
          for (Map.Entry<String, Object> key : ((JsonObject) entry.getValue())) {
            config.addDataSourceProperty(key.getKey(), key.getValue());
          }
          break;
      }
    }

    return new HikariDataSource(config);
  }

  private long getLong(Object value) {
    if (value.getClass() == Long.class || value.getClass() == Long.TYPE) {
      return (Long) value;
    }
    if (value.getClass() == Integer.class || value.getClass() == Integer.TYPE) {
      return Long.valueOf((Integer) value);
    }
    throw new IllegalArgumentException("Invalid value to be cast to long: " + value);
  }

  @Override
  public int maximumPoolSize(DataSource dataSource, JsonObject config) throws SQLException {
    if (dataSource instanceof HikariDataSource) {
      return ((HikariDataSource)dataSource).getMaximumPoolSize();
    }
    return -1;
  }

  @Override
  public void close(DataSource dataSource) throws SQLException {
    if (dataSource instanceof HikariDataSource) {
      ((HikariDataSource) dataSource).close();
    }
  }
}
