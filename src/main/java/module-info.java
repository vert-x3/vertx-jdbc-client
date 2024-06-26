import io.vertx.jdbcclient.spi.JDBCDecoder;
import io.vertx.jdbcclient.spi.JDBCEncoder;

module io.vertx.client.jdbc {
  requires io.vertx.core.logging;
  requires transitive io.vertx.client.sql;
  requires static io.vertx.codegen.api;
  requires static io.vertx.codegen.json;
  requires java.sql;
  requires static vertx.docgen;
  exports io.vertx.jdbcclient;
  exports io.vertx.jdbcclient.spi;
  uses JDBCEncoder;
  uses JDBCDecoder;
}
