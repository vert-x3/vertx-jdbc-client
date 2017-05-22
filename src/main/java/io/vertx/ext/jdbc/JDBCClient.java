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

package io.vertx.ext.jdbc;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.impl.JDBCClientImpl;
import io.vertx.ext.sql.SQLClient;

import javax.sql.DataSource;
import java.util.UUID;

/**
 * An asynchronous client interface for interacting with a JDBC compliant database
 *
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
@VertxGen
public interface JDBCClient extends SQLClient {

  /**
   * The default data source provider is C3P0
   */
  String DEFAULT_PROVIDER_CLASS =  "io.vertx.ext.jdbc.spi.impl.C3P0DataSourceProvider";

  /**
   * The name of the default data source
   */
  String DEFAULT_DS_NAME = "DEFAULT_DS";

  /**
   * Create a JDBC client which maintains its own data source.
   *
   * @param vertx  the Vert.x instance
   * @param config  the configuration
   * @return the client
   */
  static JDBCClient createNonShared(Vertx vertx, JsonObject config) {
    return new JDBCClientImpl(vertx, config, UUID.randomUUID().toString());
  }

  /**
   * Create a JDBC client which shares its data source with any other JDBC clients created with the same
   * data source name
   *
   * @param vertx  the Vert.x instance
   * @param config  the configuration
   * @param dataSourceName  the data source name
   * @return the client
   */
  static JDBCClient createShared(Vertx vertx, JsonObject config, String dataSourceName) {
    return new JDBCClientImpl(vertx, config, dataSourceName);
  }

  /**
   * Like {@link #createShared(io.vertx.core.Vertx, JsonObject, String)} but with the default data source name
   * @param vertx  the Vert.x instance
   * @param config  the configuration
   * @return the client
   */
  static JDBCClient createShared(Vertx vertx, JsonObject config) {
    return new JDBCClientImpl(vertx, config, DEFAULT_DS_NAME);
  }

  /**
   * Create a client using a pre-existing data source
   *
   * @param vertx  the Vert.x instance
   * @param dataSource  the datasource
   * @return the client
   */
  @GenIgnore
  static JDBCClient create(Vertx vertx, DataSource dataSource) {
    return new JDBCClientImpl(vertx, dataSource);
  }
}
