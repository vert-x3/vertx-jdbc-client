package io.vertx.ext.jdbc;

import io.vertx.core.json.JsonObject;

public class DBConfigs {

  public static JsonObject derby(Class<?> clazz) {
    return new JsonObject()
            .put("url", "jdbc:derby:memory:" + clazz.getSimpleName() + ";create=true")
            .put("driver_class", "org.apache.derby.jdbc.EmbeddedDriver");
  }

  public static JsonObject h2(Class<?> clazz) {
    return new JsonObject()
        .put("url", "jdbc:h2:mem:" + clazz.getSimpleName() + "?shutdown=true;DB_CLOSE_DELAY=-1")
        .put("driver_class", "org.h2.Driver");
  }

  public static JsonObject mysql(Class<?> clazz) {
    return new JsonObject()
        .put("url", "jdbc:mysql://localhost/" + clazz.getSimpleName())
        .put("driver_class", "com.mysql.jdbc.Driver")
        .put("user", "root")
        .put("password", "mypassword");
  }

  public static JsonObject hsqldb(Class<?> clazz) {
    return new JsonObject()
      .put("url", "jdbc:hsqldb:mem:" + clazz.getSimpleName() + "?shutdown=true")
      .put("driver_class", "org.hsqldb.jdbcDriver");
  }
}
