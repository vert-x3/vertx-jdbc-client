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

package io.vertx.ext.jdbc.spi.impl;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;
import com.mchange.v2.c3p0.cfg.C3P0Config;
import com.mchange.v2.c3p0.impl.C3P0Defaults;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.SQLException;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class C3P0DataSourceProvider implements DataSourceProvider {
  @Override
  public DataSource getDataSource(JsonObject config) throws SQLException {
    String url = config.getString("url");
    if (url == null) throw new NullPointerException("url cannot be null");
    String driverClass = config.getString("driver_class");
    String user = config.getString("user");
    String password = config.getString("password");
    Integer maxPoolSize = config.getInteger("max_pool_size");
    Integer initialPoolSize = config.getInteger("initial_pool_size");
    Integer minPoolSize = config.getInteger("min_pool_size");
    Integer maxStatements = config.getInteger("max_statements");
    Integer maxStatementsPerConnection = config.getInteger("max_statements_per_connection");
    Integer maxIdleTime = config.getInteger("max_idle_time");
    Integer acquireRetryAttempts = config.getInteger("acquire_retry_attempts");
    Integer acquireRetryDelay = config.getInteger("acquire_retry_delay");
    Boolean breakAfterAcquireFailure = config.getBoolean("break_after_acquire_failure");

    // If you want to configure any other C3P0 properties you can add a file c3p0.properties to the classpath
    ComboPooledDataSource cpds = new ComboPooledDataSource();
    cpds.setJdbcUrl(url);
    if (driverClass != null) {
      try {
        cpds.setDriverClass(driverClass);
      } catch (PropertyVetoException e) {
        throw new IllegalArgumentException(e);
      }
    }
    if (user != null) {
      cpds.setUser(user);
    }
    if (password != null) {
      cpds.setPassword(password);
    }
    if (maxPoolSize != null) {
      cpds.setMaxPoolSize(maxPoolSize);
    }
    if (minPoolSize != null) {
      cpds.setMinPoolSize(minPoolSize);
    }
    if (initialPoolSize != null) {
      cpds.setInitialPoolSize(initialPoolSize);
    }
    if (maxStatements != null) {
      cpds.setMaxStatements(maxStatements);
    }
    if (maxStatementsPerConnection != null) {
      cpds.setMaxStatementsPerConnection(maxStatementsPerConnection);
    }
    if (maxIdleTime != null) {
      cpds.setMaxIdleTime(maxIdleTime);
    }
    if(acquireRetryAttempts != null){
      cpds.setAcquireRetryAttempts(acquireRetryAttempts);
    }
    if(acquireRetryDelay != null){
      cpds.setAcquireRetryDelay(acquireRetryDelay);
    }
    if(breakAfterAcquireFailure != null){
      cpds.setBreakAfterAcquireFailure(breakAfterAcquireFailure);
    }
    return cpds;
  }

  @Override
  public void close(DataSource dataSource) throws SQLException {
    if (dataSource instanceof PooledDataSource) {
      ((PooledDataSource) dataSource).close();
    }
  }

  @Override
  public int maximumPoolSize(DataSource dataSource, JsonObject config) throws SQLException {
    if (dataSource instanceof PooledDataSource) {
      Integer val = config.getInteger("max_pool_size");
      if (val == null) {
        val =  C3P0Config.initializeIntPropertyVar("maxPoolSize", C3P0Defaults.maxPoolSize());
      }
      return val;
    }
    return -1;
  }
}
