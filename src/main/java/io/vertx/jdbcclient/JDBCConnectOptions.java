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

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject(generateConverter = true)
public class JDBCConnectOptions {

  private String dataSourceImplementation = "AGROAL";
  private boolean metricsEnabled;
  private String jdbcUrl;
  private String user;
  private String password;
  private String database;
  private int connectTimeout = 60000;
  private int idleTimeout;

  public JDBCConnectOptions() {}

  public JDBCConnectOptions(JsonObject json) {
    JDBCConnectOptionsConverter.fromJson(json, this);
  }

  public String getDataSourceImplementation() {
    return dataSourceImplementation;
  }

  public JDBCConnectOptions setDataSourceImplementation(String dataSourceImplementation) {
    this.dataSourceImplementation = dataSourceImplementation;
    return this;
  }

  public boolean isMetricsEnabled() {
    return metricsEnabled;
  }

  public JDBCConnectOptions setMetricsEnabled(boolean metricsEnabled) {
    this.metricsEnabled = metricsEnabled;
    return this;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public JDBCConnectOptions setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
    return this;
  }

  public String getUser() {
    return user;
  }

  public JDBCConnectOptions setUser(String user) {
    this.user = user;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public JDBCConnectOptions setPassword(String password) {
    this.password = password;
    return this;
  }

  public String getDatabase() {
    return database;
  }

  public JDBCConnectOptions setDatabase(String database) {
    this.database = database;
    return this;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public JDBCConnectOptions setConnectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  public int getIdleTimeout() {
    return idleTimeout;
  }

  public JDBCConnectOptions setIdleTimeout(int idleTimeout) {
    this.idleTimeout = idleTimeout;
    return this;
  }

  public JsonObject toJson() {
    final JsonObject json = new JsonObject();
    JDBCConnectOptionsConverter.toJson(this, json);
    return json;
  }
}
