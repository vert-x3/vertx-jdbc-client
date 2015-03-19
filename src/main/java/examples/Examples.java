package examples;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JdbcService;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SqlConnection;

import javax.sql.DataSource;

/**
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Examples {

  public void example1(Vertx vertx) {

    // Deploy service - can be anywhere on your network
    JsonObject config = new JsonObject().put("host", "mymysqldb.mycompany");
    DeploymentOptions options = new DeploymentOptions().setConfig(config);

    vertx.deployVerticle("service:io.vertx:vertx-jdbc-service", options, res -> {
      if (res.succeeded()) {
        // Deployed ok
      } else {
        // Failed to deploy
      }
    });
  }

  public void example2(Vertx vertx) {

    JdbcService proxy = JdbcService.createEventBusProxy(vertx, "vertx.jdbc");

    // Now do stuff with it:

    proxy.getConnection(res -> {
      if (res.succeeded()) {

        SqlConnection connection = res.result();

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

  public void example3(Vertx vertx) {

    JsonObject config = new JsonObject().put("host", "mymysqldb.mycompany");

    JdbcService jdbcService = JdbcService.create(vertx, config);

    jdbcService.start();

  }

  public void example3_1(Vertx vertx, DataSource myDataSource) {

    JsonObject config = new JsonObject().put("host", "mymysqldb.mycompany");

    JdbcService jdbcService = JdbcService.create(vertx, config, myDataSource);

    jdbcService.start();

  }

  public void example4(JdbcService service) {

    // Now do stuff with it:

    service.getConnection(res -> {
      if (res.succeeded()) {

        SqlConnection connection = res.result();

        // Got a connection

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

    JdbcService service = JdbcService.create(vertx, config);

    service.start();
  }
}
