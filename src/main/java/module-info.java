module io.vertx.sql.client.jdbc {

  requires static io.vertx.codegen.api;
  requires static io.vertx.codegen.json;
  requires static io.vertx.docgen;

  requires io.vertx.core;
  requires io.vertx.core.logging;
  requires io.vertx.sql.client;
  requires java.sql;

  exports io.vertx.jdbcclient;
  exports io.vertx.jdbcclient.spi;

  uses io.vertx.jdbcclient.spi.JDBCEncoder;
  uses io.vertx.jdbcclient.spi.JDBCDecoder;

}
