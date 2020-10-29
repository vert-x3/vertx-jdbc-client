package io.vertx.jdbcclient;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
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
        case "connectTimeout":
          if (member.getValue() instanceof Number) {
            obj.setConnectTimeout(((Number)member.getValue()).intValue());
          }
          break;
        case "dataSourceImplementation":
          if (member.getValue() instanceof String) {
            obj.setDataSourceImplementation((String)member.getValue());
          }
          break;
        case "database":
          if (member.getValue() instanceof String) {
            obj.setDatabase((String)member.getValue());
          }
          break;
        case "idleTimeout":
          if (member.getValue() instanceof Number) {
            obj.setIdleTimeout(((Number)member.getValue()).intValue());
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
        case "password":
          if (member.getValue() instanceof String) {
            obj.setPassword((String)member.getValue());
          }
          break;
        case "tracingPolicy":
          if (member.getValue() instanceof String) {
            obj.setTracingPolicy(io.vertx.core.tracing.TracingPolicy.valueOf((String)member.getValue()));
          }
          break;
        case "user":
          if (member.getValue() instanceof String) {
            obj.setUser((String)member.getValue());
          }
          break;
      }
    }
  }

  public static void toJson(JDBCConnectOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(JDBCConnectOptions obj, java.util.Map<String, Object> json) {
    json.put("connectTimeout", obj.getConnectTimeout());
    if (obj.getDataSourceImplementation() != null) {
      json.put("dataSourceImplementation", obj.getDataSourceImplementation());
    }
    if (obj.getDatabase() != null) {
      json.put("database", obj.getDatabase());
    }
    json.put("idleTimeout", obj.getIdleTimeout());
    if (obj.getJdbcUrl() != null) {
      json.put("jdbcUrl", obj.getJdbcUrl());
    }
    json.put("metricsEnabled", obj.isMetricsEnabled());
    if (obj.getPassword() != null) {
      json.put("password", obj.getPassword());
    }
    if (obj.getTracingPolicy() != null) {
      json.put("tracingPolicy", obj.getTracingPolicy().name());
    }
    if (obj.getUser() != null) {
      json.put("user", obj.getUser());
    }
  }
}
