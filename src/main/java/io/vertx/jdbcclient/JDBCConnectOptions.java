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
import io.vertx.sqlclient.SqlConnectOptions;

@DataObject(generateConverter = true)
public class JDBCConnectOptions extends SqlConnectOptions {

  private String dataSourceImplementation = "AGROAL";
  private boolean metricsEnabled;
  private String jdbcUrl;

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

  public JsonObject toJson() {
    final JsonObject json = new JsonObject();
    JDBCConnectOptionsConverter.toJson(this, json);
    return json;
  }
}
