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

package io.vertx.ext.jdbc.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.ServiceHelper;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.ext.jdbc.JdbcConnection;
import io.vertx.ext.jdbc.JdbcService;
import io.vertx.ext.jdbc.RuntimeSqlException;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JdbcServiceImpl implements JdbcService {

  private static final Logger log = LoggerFactory.getLogger(JdbcService.class);

  private final Vertx vertx;
  private final JsonObject config;
  private final long txTimeout;

  //TODO: Connection pool, port mod-jdbc-persistor
  private DataSourceProvider provider;
  private DataSource dataSource;

  public JdbcServiceImpl(Vertx vertx, JsonObject config) {
    this.vertx = vertx;
    this.config = config;
    this.txTimeout = config.getInteger("txTimeout", 10000);
  }

  @Override
  public void start() {
    provider = ServiceHelper.loadFactory(DataSourceProvider.class);
    try {
      dataSource = provider.getDataSource(config);
    } catch (SQLException e) {
      throw new RuntimeSqlException(e);
    }
  }

  @Override
  public void stop() {
    if (provider != null) {
      try {
        provider.close(dataSource);
      } catch (SQLException e) {
        log.error("Exception occurred trying to close out the data source", e);
      }
    }
  }

  @Override
  public void getConnection(Handler<AsyncResult<JdbcConnection>> handler) {
    try {
      JdbcConnection conn = new JdbcConnectionImpl(vertx, dataSource.getConnection());
      handler.handle(Future.succeededFuture(conn));
    } catch (Throwable t) {
      handler.handle(Future.failedFuture(t));
    }
  }
}
