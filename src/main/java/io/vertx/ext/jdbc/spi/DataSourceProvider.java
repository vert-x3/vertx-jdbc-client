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

package io.vertx.ext.jdbc.spi;

import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;
import javax.sql.DataSource;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.sqlclient.PoolOptions;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public interface DataSourceProvider {

  /**
   * Transforms the {@link JDBCConnectOptions} and {@link PoolOptions} into a config suitable to be passed
   * as {@link #create(JsonObject)} argument.
   */
  default JsonObject toJson(JDBCConnectOptions connectOptions, PoolOptions poolOptions) {
    return new JsonObject();
  }

  /**
   * Init provider with specific configuration
   *
   * @param sqlConfig SQL connection configuration
   * @return a reference to this for fluent API
   * @apiNote Use it conjunction with {@link #create(JsonObject)}
   * @since 4.2.0
   */
  default DataSourceProvider init(JsonObject sqlConfig) {
    return this;
  }

  /**
   * Get the SQL initial configuration
   *
   * @return an initial configuration
   * @apiNote Use it conjunction with {@link #init(JsonObject)}
   * @since 4.2.0
   */
  default JsonObject getInitialConfig() {
    return new JsonObject();
  }

  int maximumPoolSize(DataSource dataSource, JsonObject config) throws SQLException;

  DataSource getDataSource(JsonObject config) throws SQLException;

  void close(DataSource dataSource) throws SQLException;

  static DataSourceProvider create(JsonObject config) {
    String providerClass = config.getString("provider_class");
    if (providerClass == null) {
      providerClass = JDBCClient.DEFAULT_PROVIDER_CLASS;
    }

    if (Thread.currentThread().getContextClassLoader() != null) {
      try {
        // Try with the TCCL
        Class clazz = Thread.currentThread().getContextClassLoader().loadClass(providerClass);
        return ((DataSourceProvider) clazz.newInstance()).init(config);
      } catch (ClassNotFoundException e) {
        // Next try.
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      // Try with the classloader of the current class.
      Class clazz = DataSourceProvider.class.getClassLoader().loadClass(providerClass);
      return ((DataSourceProvider) clazz.newInstance()).init(config);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Init provider with specific {@link DataSource} and config. The config expects that several properties are set:
   *
   * <ul>
   *   <li>{@code url} - the connection string</li>
   *   <li>{@code user} - the connection user name</li>
   *   <li>{@code database} - the database name</li>
   *   <li>{@code maxPoolSize} - the max allowed number of connections in the pool</li>
   * </ul>
   *
   * @param dataSource a pre initialized data source
   * @param config the configuration for the datasource
   * @return a reference to this for fluent API
   * @since 4.2.0
   */
  static DataSourceProvider create(final DataSource dataSource, final JsonObject config) {
    Objects.requireNonNull(config, "config must not be null");

    return new DataSourceProvider() {

      @Override
      public JsonObject getInitialConfig() {
        return config;
      }

      @Override
      public int maximumPoolSize(DataSource arg0, JsonObject arg1) {
        return config.getInteger("maxPoolSize", -1);
      }

      @Override
      public DataSource getDataSource(JsonObject arg0) {
        return dataSource;
      }

      @Override
      public void close(DataSource arg0) throws SQLException {
        if (dataSource instanceof AutoCloseable) {
          try {
            ((AutoCloseable) dataSource).close();
          } catch (Exception e) {
            throw new SQLException("Failed to close data source", e);
          }
        }
      }
    };
  }
}
