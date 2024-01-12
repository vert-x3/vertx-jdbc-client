package io.vertx.jdbcclient;

public class DataSourceConfigs {

  public static JDBCConnectOptions derby(Class<?> clazz) {
    return new JDBCConnectOptions()
            .setJdbcUrl("jdbc:derby:memory:" + clazz.getSimpleName() + ";create=true");
//            ("driver_class", "org.apache.derby.jdbc.EmbeddedDriver");
  }

  public static JDBCConnectOptions h2(Class<?> clazz) {
    return new JDBCConnectOptions()
        .setJdbcUrl("jdbc:h2:mem:" + clazz.getSimpleName() + "?shutdown=true;DB_CLOSE_DELAY=-1");
//        .put("driver_class", "org.h2.Driver");
  }

  public static JDBCConnectOptions mysql(Class<?> clazz) {
    return new JDBCConnectOptions()
        .setJdbcUrl("jdbc:mysql://localhost/" + clazz.getSimpleName())
//        .put("driver_class", "com.mysql.jdbc.Driver")
        .setUser("root")
        .setPassword("mypassword");
  }

  public static JDBCConnectOptions hsqldb(Class<?> clazz) {
    return new JDBCConnectOptions()
      .setUser("jdbc:hsqldb:mem:" + clazz.getSimpleName() + "?shutdown=true");
//      .put("driver_class", "org.hsqldb.jdbcDriver");
  }
}
