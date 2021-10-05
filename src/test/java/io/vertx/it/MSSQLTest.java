package io.vertx.it;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.jdbcclient.SqlOutParam;
import io.vertx.sqlclient.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.JDBCType;
import java.util.ArrayList;

@RunWith(VertxUnitRunner.class)
public class MSSQLTest {

  @Rule
  public final RunTestOnContext rule = new RunTestOnContext();

  private MSSQLServer server;
  protected JDBCPool client;

  @Before
  public void before(TestContext should) {
    final Async test = should.async();
    rule.vertx().executeBlocking(p -> {
      try {
        server = new MSSQLServer();
        server.withInitScript("init-mssql.sql");
        server.start();
        p.complete();
      } catch (RuntimeException e) {
        p.fail(e);
      }
    }, true, init -> {
      if (init.succeeded()) {
        JDBCConnectOptions options = new JDBCConnectOptions()
          .setJdbcUrl(server.getJdbcUrl())
          .setUser(server.getUsername())
          .setPassword(server.getPassword());

        client = JDBCPool.pool(rule.vertx(), options, new PoolOptions().setMaxSize(1));
        test.complete();
      } else {
        should.fail(init.cause());
      }
    });
  }

  @After
  public void after() {
    server.close();
  }

  private static class MSSQLServer extends MSSQLServerContainer {
    @Override
    protected void configure() {
      this.addExposedPort(MSSQLServerContainer.MS_SQL_SERVER_PORT);
      this.addEnv("ACCEPT_EULA", "Y");
      this.addEnv("SA_PASSWORD", this.getPassword());
    }
  }

  @Test
  public void simpleTest(TestContext should) {
    final Async test = should.async();
    // this test would fail if we would attempt to read the generated ids after the end of the cursor
    // the fix implies that we must read them before we close the cursor.
    client
      .preparedQuery("select * from Fortune")
      .execute(should.asyncAssertSuccess(resultSet -> {
        should.assertEquals(12, resultSet.size());
        test.complete();
      }));
  }

  @Test
  public void simpleRSAfterUpdate(TestContext should) {
    final Async test = should.async();
    client
      .preparedQuery(//"set nocount on;\n" +
        "INSERT INTO test (field1)\n" +
        "SELECT ?")
      .executeBatch(new ArrayList<Tuple>() {{
        Tuple.of(1);
      }})
      .onFailure(should::fail)
      .onSuccess(rowSet -> {
        test.complete();
      });
  }

  @Test
  public void testProcedures(TestContext should) {
    final Async test = should.async();

    client
      .preparedQuery("{ call rsp_vertx_test_1(?, ?)}")
      .execute(Tuple.of(1, SqlOutParam.OUT(JDBCType.VARCHAR)))
        .onFailure(should::fail)
          .onSuccess(rows -> {
            // assert that the first result is received
            should.assertNotNull(rows);
            should.assertTrue(rows.size() > 0);
            for (Row row : rows) {
              should.assertNotNull(row);
            }
            // process the next response
            rows = rows.next();
            should.assertNotNull(rows);
            should.assertTrue(rows.property(JDBCPool.OUTPUT));
            should.assertTrue(rows.size() > 0);
            for (Row row : rows) {
              should.assertEquals("echo", row.getString(0));
            }
            test.complete();
          });
  }

  @Test
  public void testProcedures2(TestContext should) {
    final Async test = should.async();

    client
      .preparedQuery("{ call rsp_vertx_test_2(?)}")
      .execute(Tuple.of(SqlOutParam.OUT(JDBCType.VARCHAR)))
      .onFailure(should::fail)
      .onSuccess(rows -> {
        should.assertNotNull(rows);
        should.assertTrue(rows.property(JDBCPool.OUTPUT));
        should.assertTrue(rows.size() > 0);
        for (Row row : rows) {
          should.assertEquals("echo", row.getString(0));
        }
        test.complete();
      });
  }
}
