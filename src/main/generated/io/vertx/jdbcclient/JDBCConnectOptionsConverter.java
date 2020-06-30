package io.vertx.jdbcclient;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.jdbcclient.JDBCConnectOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.jdbcclient.JDBCConnectOptions} original class using Vert.x codegen.
 */
public class JDBCConnectOptionsConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, JDBCConnectOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "dataSourceImplementation":
          if (member.getValue() instanceof String) {
            obj.setDataSourceImplementation((String)member.getValue());
          }
          break;
        case "jdbcUrl":
          if (member.getValue() instanceof String) {
            obj.setJdbcUrl((String)member.getValue());
          }
          break;
        case "metricsEnabled":
          if (member.getValue() instanceof Boolean) {
            obj.setMetricsEnabled((Boolean)member.getValue());
          }
          break;
      }
    }
  }

  public static void toJson(JDBCConnectOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(JDBCConnectOptions obj, java.util.Map<String, Object> json) {
    if (obj.getDataSourceImplementation() != null) {
      json.put("dataSourceImplementation", obj.getDataSourceImplementation());
    }
    if (obj.getJdbcUrl() != null) {
      json.put("jdbcUrl", obj.getJdbcUrl());
    }
    json.put("metricsEnabled", obj.isMetricsEnabled());
  }
}
