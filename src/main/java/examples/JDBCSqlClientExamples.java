package examples;

import io.vertx.core.Vertx;
import io.vertx.docgen.Source;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.jdbcclient.SqlOutParam;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import java.sql.JDBCType;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
@Source
public class JDBCSqlClientExamples {

  public void exampleCreateDefault(Vertx vertx) {
    JDBCConnectOptions connectOptions = new JDBCConnectOptions()
      .setJdbcUrl("jdbc:h2:~/test")
      .setUser("sa")
      .setPassword("");
    PoolOptions poolOptions = new PoolOptions()
      .setMaxSize(16);
    Pool pool = JDBCPool.pool(vertx, connectOptions, poolOptions);
  }

  public void exampleCreateTypeSafe(Vertx vertx) {

    Pool pool = JDBCPool.pool(
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
        .setName("pool-name")
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

  public void exampleCallableIN(JDBCPool pool) {

    // callable statements must ad-here to the JDBC spec
    // JDBC defines that callable statements are enclosed by { ... }
    // and have a special "call" command
    String sql = "{call new_customer(?, ?)}";

    pool
      .preparedQuery(sql)
      // by default the "IN" argument types are extracted from the
      // type of the data in the tuple, as well as the order
      .execute(Tuple.of("Paulo", "Lopes"))
      .onFailure(e -> {
        // handle the failure
      })
      .onSuccess(rows -> {
        // ... success handler
      });
  }

  public void exampleCallableINOUT(JDBCPool pool) {

    // callable statements must ad-here to the JDBC spec
    // JDBC defines that callable statements are enclosed by { ... }
    // and have a special "call" command
    String sql = "{call customer_lastname(?, ?)}";

    pool
      .preparedQuery(sql)
      // by default the "IN" argument types are extracted from the
      // type of the data in the tuple, as well as the order
      //
      // Note that we now also declare the output parameter and it's
      // type. The type can be a "String", "int" or "JDBCType" constant
      .execute(Tuple.of("John", SqlOutParam.OUT(JDBCType.VARCHAR)))
      .onFailure(e -> {
        // handle the failure
      })
      .onSuccess(rows -> {
        // we can verify if there was a output received from the callable statement
        if (rows.property(JDBCPool.OUTPUT)) {
          // and then iterate the results
          for (Row row : rows) {
            System.out.println(row.getString(0));
          }
        }
      });
  }

  public void exampleOutParam() {
    SqlOutParam param;

    // map IN as "int" as well as "OUT" as VARCHAR
    param = SqlOutParam.INOUT(123456, JDBCType.VARCHAR);
    // or
    param = SqlOutParam.INOUT(123456, "VARCHAR");

    // and then just add to the tuple as usual:
    Tuple.of(param);
  }

  public void exampleGeneratedKeys(JDBCPool pool) {

    String sql = "INSERT INTO insert_table (FNAME, LNAME) VALUES (?, ?)";

    pool
      .preparedQuery(sql)
      .execute(Tuple.of("Paulo", "Lopes"))
      .onSuccess(rows -> {
        // the generated keys are returned as an extra row
        Row lastInsertId = rows.property(JDBCPool.GENERATED_KEYS);
        // just refer to the position as usual:
        long newId = lastInsertId.getLong(0);
      });
  }
}
