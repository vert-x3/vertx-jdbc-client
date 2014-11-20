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
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.ext.jdbc.JdbcService;
import io.vertx.ext.jdbc.RuntimeSqlException;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JdbcServiceImpl implements JdbcService {

  private static final Logger log = LoggerFactory.getLogger(JdbcService.class);

  private final Vertx vertx;
  private final JsonObject config;

  //TODO: Connection pool, port mod-jdbc-persistor
  private Driver driver;
  private String url;
  private TimerEvictedConcurrentMap<String, Connection> transactions;

  public JdbcServiceImpl(Vertx vertx, JsonObject config) {
    this.vertx = vertx;
    this.config = config;
  }

  @Override
  public void start() {
    String driverName = config.getString("driver");
    //TODO: Does illegal arg exception make sense here ? I think we need a ConfigurationException or something for verticles/services
    if (driverName == null) throw new IllegalArgumentException("'driver' is required to configure this service");

    url = config.getString("url");
    if (url == null) throw new IllegalArgumentException("'url' is required to configure this service");

    try {
      driver = DriverManager.getDriver(url);
    } catch (Throwable t) {
      throw new RuntimeException("Error initializing JDBC service", t);
    }
  }

  @Override
  public void stop() {

  }

  @Override
  public void execute(String sql, Handler<AsyncResult<Void>> resultHandler) throws RuntimeSqlException {
    try {
      Connection conn = driver.connect(url, new Properties());
      Statement stmt = conn.createStatement();
      stmt.execute(sql);
      complete(resultHandler);
    } catch (SQLException e) {
      error(e, resultHandler);
    }
  }

  @Override
  public void executeWithTx(String transactionId, String sql, Handler<AsyncResult<Void>> resultHandler) throws RuntimeSqlException {

  }

  @Override
  public void select(String table, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
    doSelect(table, "*", resultHandler);
  }

  @Override
  public void selectTx(String transactionId, String table, Handler<AsyncResult<JsonObject>> resultHandler) {
  }

  //@Override
  public void selectWithFields(String table, List<String> fields, Handler<AsyncResult<List<JsonObject>>> resultHandler) {

  }

  //@Override
  public void selectWithFieldsTx(String transactionId, String table, List<String> fields, Handler<AsyncResult<JsonObject>> resultHandler) {
  }

  @Override
  public void commit(String transactionId) {

  }

  @Override
  public void rollback(String transactionId) {

  }

  private void doSelect(String table, String select, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
    try {
      Connection conn = driver.connect(url, new Properties());
      PreparedStatement ps = conn.prepareStatement("SELECT " + select + " FROM " + table);
      ResultSet rs = ps.executeQuery();
      List<JsonObject> list = new ArrayList<>();
      while (rs.next()) {
        JsonObject json = new JsonObject();
        ResultSetMetaData metaData = rs.getMetaData();
        int cols = metaData.getColumnCount();
        for (int i = 1; i <= cols; i++) {
          json.put(metaData.getColumnName(i), rs.getObject(i));
        }
        list.add(json);
      }
      complete(list, resultHandler);
    } catch (SQLException e) {
      error(e, resultHandler);
    }
  }

  private void complete(Handler<AsyncResult<Void>> handler) {
    complete(null, handler);
  }

  private <T> void complete(T result, Handler<AsyncResult<T>> handler) {
    handler.handle(Future.completedFuture(result));
  }

  private <T> void error(Throwable t, Handler<AsyncResult<T>> handler) {
    handler.handle(Future.completedFuture(t));
  }
}
