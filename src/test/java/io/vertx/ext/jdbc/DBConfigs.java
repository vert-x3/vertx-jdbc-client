package io.vertx.ext.jdbc;

import io.vertx.core.json.JsonObject;

public class DBConfigs {

  public static JsonObject derby() {
    return new JsonObject()
            .put("url", "jdbc:derby:memory:myDB2;create=true")
            .put("driver_class", "org.apache.derby.jdbc.EmbeddedDriver");
  }

  public static JsonObject h2() {
    return new JsonObject()
        .put("url", "jdbc:h2:mem:test?shutdown=true")
        .put("driver_class", "org.h2.Driver");
  }

  public static JsonObject mysql() {
    return new JsonObject()
        .put("url", "jdbc:mysql://localhost/test")
        .put("driver_class", "com.mysql.jdbc.Driver")
        .put("user", "root")
        .put("password", "mypassword");
  }

  public static JsonObject hsqldb() {
    return new JsonObject()
      .put("url", "jdbc:hsqldb:mem:test?shutdown=true")
      .put("driver_class", "org.hsqldb.jdbcDriver");
  }
}
