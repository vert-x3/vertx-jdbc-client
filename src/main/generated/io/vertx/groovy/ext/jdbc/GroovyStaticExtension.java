package io.vertx.groovy.ext.jdbc;
public class GroovyStaticExtension {
  public static io.vertx.ext.jdbc.JDBCClient createNonShared(io.vertx.ext.jdbc.JDBCClient j_receiver, io.vertx.core.Vertx vertx, java.util.Map<String, Object> config) {
    return io.vertx.lang.groovy.ConversionHelper.wrap(io.vertx.ext.jdbc.JDBCClient.createNonShared(vertx,
      config != null ? io.vertx.lang.groovy.ConversionHelper.toJsonObject(config) : null));
  }
  public static io.vertx.ext.jdbc.JDBCClient createShared(io.vertx.ext.jdbc.JDBCClient j_receiver, io.vertx.core.Vertx vertx, java.util.Map<String, Object> config, java.lang.String dataSourceName) {
    return io.vertx.lang.groovy.ConversionHelper.wrap(io.vertx.ext.jdbc.JDBCClient.createShared(vertx,
      config != null ? io.vertx.lang.groovy.ConversionHelper.toJsonObject(config) : null,
      dataSourceName));
  }
  public static io.vertx.ext.jdbc.JDBCClient createShared(io.vertx.ext.jdbc.JDBCClient j_receiver, io.vertx.core.Vertx vertx, java.util.Map<String, Object> config) {
    return io.vertx.lang.groovy.ConversionHelper.wrap(io.vertx.ext.jdbc.JDBCClient.createShared(vertx,
      config != null ? io.vertx.lang.groovy.ConversionHelper.toJsonObject(config) : null));
  }
}
