package examples;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.docgen.Source;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import javax.sql.DataSource;

/**
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
@Source
public class JDBCExamples {

  public void exampleCreateDefault(Vertx vertx, JsonObject config) {

    SQLClient client = JDBCClient.createShared(vertx, config);

  }

  public void exampleCreateDataSourceName(Vertx vertx, JsonObject config) {


    SQLClient client = JDBCClient.createShared(vertx, config, "MyDataSource");

  }

  public void exampleCreateWithDataSource(Vertx vertx, DataSource dataSource) {

    SQLClient client = JDBCClient.create(vertx, dataSource);

  }

  public void exampleCreateNonShared(Vertx vertx, JsonObject config) {

    SQLClient client = JDBCClient.createNonShared(vertx, config);

  }

  public void example4(JDBCClient client) {

    // Now do stuff with it:

    client.getConnection(res -> {
      if (res.succeeded()) {

        SQLConnection connection = res.result();

        connection.query("SELECT * FROM some_table", res2 -> {
          if (res2.succeeded()) {

            ResultSet rs = res2.result();
            // Do something with results
          }
        });
      } else {
        // Failed to get connection - deal with it
      }
    });

  }

  public void example5(Vertx vertx) {

    JsonObject config = new JsonObject()
      .put("url", "jdbc:hsqldb:mem:test?shutdown=true")
      .put("driver_class", "org.hsqldb.jdbcDriver")
      .put("max_pool_size", 30);

    SQLClient client = JDBCClient.createShared(vertx, config);

  }
}
