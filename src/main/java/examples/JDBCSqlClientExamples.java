package examples;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.docgen.Source;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import javax.sql.DataSource;
import java.time.Instant;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
@Source
public class JDBCSqlClientExamples {

  public void exampleCreateDefault(Vertx vertx, JsonObject config) {
    JDBCPool pool = JDBCPool.pool(vertx, config);
  }

  public void exampleCreateTypeSafe(Vertx vertx) {

    JDBCPool pool = JDBCPool.pool(
      vertx,
      // configure the connection
      new JDBCConnectOptions()
        // H2 connection string
        .setJdbcUrl("jdbc:h2:~/test")
        // username
        .setUser("sa")
        // password
        .setPassword(""),
      // configure the pool
      new PoolOptions()
        .setMaxSize(16)
    );

  }

  public void exampleQueryManaged(JDBCPool pool) {

    pool
      .query("SELECT * FROM user")
      .execute()
      .onFailure(e -> {
        // handle the failure
      })
      .onSuccess(rows -> {
        for (Row row : rows) {
          System.out.println(row.getString("FIRST_NAME"));
        }
      });
  }

  public void examplePreparedQueryManaged(JDBCPool pool) {

    pool
      .preparedQuery("SELECT * FROM user WHERE emp_id > ?")
      // the emp id to look up
      .execute(Tuple.of(1000))
      .onFailure(e -> {
        // handle the failure
      })
      .onSuccess(rows -> {
        for (Row row : rows) {
          System.out.println(row.getString("FIRST_NAME"));
        }
      });
  }

  public void exampleQueryManual(JDBCPool pool) {

    pool
      .getConnection()
      .onFailure(e -> {
        // failed to get a connection
      })
      .onSuccess(conn -> {
        conn
          .query("SELECT * FROM user")
          .execute()
          .onFailure(e -> {
            // handle the failure

            // very important! don't forget to return the connection
            conn.close();
          })
          .onSuccess(rows -> {
            for (Row row : rows) {
              System.out.println(row.getString("FIRST_NAME"));
            }

            // very important! don't forget to return the connection
            conn.close();
          });
      });
  }

  public void examplePreparedQueryManual(JDBCPool pool) {

    pool
      .getConnection()
      .onFailure(e -> {
        // failed to get a connection
      })
      .onSuccess(conn -> {
        conn
          .preparedQuery("SELECT * FROM user WHERE emp_id > ?")
          // the emp_id to look up
          .execute(Tuple.of(1000))
          .onFailure(e -> {
            // handle the failure

            // very important! don't forget to return the connection
            conn.close();
          })
          .onSuccess(rows -> {
            for (Row row : rows) {
              System.out.println(row.getString("FIRST_NAME"));
            }

            // very important! don't forget to return the connection
            conn.close();
          });
      });
  }
}
