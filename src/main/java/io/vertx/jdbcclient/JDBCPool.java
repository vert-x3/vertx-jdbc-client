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
package io.vertx.jdbcclient;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.impl.JDBCClientImpl;
import io.vertx.ext.jdbc.spi.DataSourceProvider;
import io.vertx.ext.jdbc.spi.impl.AgroalCPDataSourceProvider;
import io.vertx.ext.jdbc.spi.impl.C3P0DataSourceProvider;
import io.vertx.ext.jdbc.spi.impl.HikariCPDataSourceProvider;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.jdbcclient.impl.JDBCPoolImpl;
import io.vertx.sqlclient.*;

import javax.sql.DataSource;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * JDBCPool is the interface that allows using the Sql Client API with plain JDBC.
 */
@VertxGen
public interface JDBCPool extends Pool {

  /**
   * The default data source provider for this pool,
   * loaded from JVM system properties with the {@link DataSourceProvider#DEFAULT_DATA_SOURCE_PROVIDER_NAME} key.
   *
   * The value can be one of:
   * <ul>
   *   <li>C3P0: {@link C3P0DataSourceProvider}</li>
   *   <li>Hikari: {@link HikariCPDataSourceProvider}</li>
   *   <li>Agroal: {@link AgroalCPDataSourceProvider}</li>
   * </ul>
   *
   * When there is no JVM wide defined provider or the value is incorrect, {@link AgroalCPDataSourceProvider} is returned.
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Supplier<DataSourceProvider> DEFAULT_DATA_SOURCE_PROVIDER = DataSourceProvider.loadDefaultDataSourceProvider()
    .orElseGet(() -> AgroalCPDataSourceProvider::new);

  /**
   * The property to be used to retrieve the generated keys
   */
  PropertyKind<Row> GENERATED_KEYS = PropertyKind.create("generated-keys", Row.class);

  /**
   * The property to be used to retrieve the output of the callable statement
   */
  PropertyKind<Boolean> OUTPUT = PropertyKind.create("callable-statement-output", Boolean.class);

  /**
   * Create a JDBC pool which maintains its own data source and the {@link #DEFAULT_DATA_SOURCE_PROVIDER default data source provider}.
   *
   * @param vertx  the Vert.x instance
   * @param connectOptions the options to configure the connection
   * @param poolOptions the connection pool options
   * @return the client
   */
  static JDBCPool pool(Vertx vertx, JDBCConnectOptions connectOptions, PoolOptions poolOptions) {
    return pool(vertx, connectOptions, poolOptions, DEFAULT_DATA_SOURCE_PROVIDER.get());
  }

  /**
   * Create a JDBC pool which maintains its own data source, with the specified provider.
   * <p>
   * The datasource will be named after {@link PoolOptions#getName()}, even though the Javadoc in this class indicates that the name is only used when the {@link Pool} is shared.
   *
   * @param vertx  the Vert.x instance
   * @param connectOptions the options to configure the connection
   * @param poolOptions the connection pool options
   * @param provider the data source provider
   * @return the client
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  static JDBCPool pool(Vertx vertx, JDBCConnectOptions connectOptions, PoolOptions poolOptions, DataSourceProvider provider) {
    return new JDBCPoolImpl(
      vertx,
      new JDBCClientImpl(vertx, provider.toJson(connectOptions, poolOptions), provider, poolOptions.getName()),
      connectOptions,
      connectOptions.getUser(),
      connectOptions.getDatabase());
  }

  /**
   * Create a JDBC pool which maintains its own data source.
   *
   * @param vertx  the Vert.x instance
   * @param config the options to configure the client using the same format as {@link io.vertx.ext.jdbc.JDBCClient}
   * @return the client
   * @deprecated instead use {@link JDBCPool#pool(Vertx, JDBCConnectOptions, PoolOptions)}
   */
  @Deprecated
  static JDBCPool pool(Vertx vertx, JsonObject config) {
    final ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    String jdbcUrl = config.getString("jdbcUrl", config.getString("url"));
    String user = config.getString("username", config.getString("user"));
    String datasourceName = config.getString("datasourceName", UUID.randomUUID().toString());
    return new JDBCPoolImpl(
      vertx,
      new JDBCClientImpl(vertx, config, datasourceName),
      new SQLOptions(config), user, datasourceName);
  }

  /**
   * Create a JDBC pool which maintains its own data source.
   *
   * @param vertx  the Vert.x instance
   * @param dataSourceProvider the options to configure the client using the same format as {@link io.vertx.ext.jdbc.JDBCClient}
   * @return the client
   * @since 4.2.0
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  static JDBCPool pool(Vertx vertx, DataSourceProvider dataSourceProvider) {
    final ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    final JsonObject config = dataSourceProvider.getInitialConfig();
    String jdbcUrl = config.getString("jdbcUrl", config.getString("url"));
    String user = config.getString("username", config.getString("user"));
    String database = config.getString("database");
    if (context.tracer() != null) {
      Objects.requireNonNull(jdbcUrl, "data source url config cannot be null");
      Objects.requireNonNull(user, "data source user config cannot be null");
      Objects.requireNonNull(database, "data source database config cannot be null");
    }
    return new JDBCPoolImpl(
      vertx,
      new JDBCClientImpl(vertx, dataSourceProvider),
      new SQLOptions(config),
      user,
      database);
  }

  /**
   * Create a JDBC pool using a pre-initialized data source.
   *
   * @param vertx  the Vert.x instance
   * @param dataSource a pre-initialized data source
   * @return the client
   * @since 4.2.0
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  static JDBCPool pool(Vertx vertx, DataSource dataSource) {
    return pool(vertx, DataSourceProvider.create(dataSource, new JsonObject()));
  }

  /**
   * Create a JDBC pool using a pre-initialized data source. The config expects that at least the following properties
   * are set:
   *
   * <ul>
   *   <li>{@code url} - the connection string</li>
   *   <li>{@code user} - the connection user name</li>
   *   <li>{@code database} - the database name</li>
   *   <li>{@code maxPoolSize} - the max allowed number of connections in the pool</li>
   * </ul>
   *
   * @param vertx  the Vert.x instance
   * @param dataSource a pre-initialized data source
   * @param config the pool configuration
   * @return the client
   * @since 4.2.0
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Deprecated
  static JDBCPool pool(Vertx vertx, DataSource dataSource, JsonObject config) {
    return pool(vertx, DataSourceProvider.create(dataSource, config));
  }
}
