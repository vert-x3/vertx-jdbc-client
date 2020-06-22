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
