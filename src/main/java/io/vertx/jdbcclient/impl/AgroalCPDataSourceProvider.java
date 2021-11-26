/*
 * Copyright (c) 2011-2014 The original author or authors
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
package io.vertx.jdbcclient.impl;

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

  private final JDBCConnectOptions connectOptions;
  private final PoolOptions poolOptions;
  private JsonObject initConfig;

  public AgroalCPDataSourceProvider(JDBCConnectOptions connectOptions, PoolOptions poolOptions) {
    this.connectOptions = connectOptions;
    this.poolOptions = poolOptions;
  }

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
  public int maximumPoolSize(DataSource dataSource, JsonObject config) {
    return poolOptions.getMaxSize();
  }

  @Override
  public DataSource getDataSource(JsonObject config) throws SQLException {

    AgroalDataSourceConfigurationSupplier dataSourceConfigurationBuilder = new AgroalDataSourceConfigurationSupplier()
      .dataSourceImplementation(AgroalDataSourceConfiguration.DataSourceImplementation.valueOf(connectOptions.getDataSourceImplementation()))
      .metricsEnabled(connectOptions.isMetricsEnabled())
      .connectionPoolConfiguration(cp ->
        cp
          .validationTimeout(Duration.ofMillis(connectOptions.getConnectTimeout()))
          .minSize(0)
          .maxSize(poolOptions.getMaxSize())
          .initialSize(1)
          .acquisitionTimeout(Duration.ofMillis(connectOptions.getConnectTimeout()))
          .reapTimeout(Duration.ofMillis(connectOptions.getIdleTimeout()))
          .leakTimeout(Duration.ofMillis(connectOptions.getIdleTimeout()))
          .connectionFactoryConfiguration(cf ->
            cf
              .jdbcUrl(connectOptions.getJdbcUrl())
              .principal(connectOptions.getUser() != null ? new NamePrincipal(connectOptions.getUser()) : null)
              .credential(connectOptions.getPassword() != null ? new SimplePassword(connectOptions.getPassword()) : null)
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
