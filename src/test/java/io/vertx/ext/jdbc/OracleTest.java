package io.vertx.ext.jdbc;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Tuple;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class OracleTest {

  @Rule
  public final RunTestOnContext rule = new RunTestOnContext();

  @Test
  @Ignore("Cannot run this in CI as we can't install Oracle")
  public void testSimple(TestContext should) {
    final Async test = should.async();

    final JDBCPool pool = JDBCPool.pool(
      rule.vertx(),
      new JDBCConnectOptions()
        .setJdbcUrl("jdbc:oracle:thin:@127.0.0.1:1521:xe")
        .setUser("sys as sysdba")
        .setPassword("vertx"),
      new PoolOptions());

    pool.preparedQuery("DELETE FROM product WHERE id = ?")
      .execute(Tuple.of("missing-id"))
      .onFailure(should::fail)
      .onSuccess(rowSet -> {
        test.complete();
      });
  }

  @Test
  @Ignore("Cannot run this in CI as we can't install Oracle")
  public void testBlocking(TestContext should) {
//    CREATE OR REPLACE FUNCTION MYSCHEMA.TEST_SLEEP
//      (
//        TIME_  IN  NUMBER
//      )
//    RETURN INTEGER IS
//      BEGIN
//    DBMS_LOCK.sleep(seconds => TIME_);
//    RETURN 1;
//    EXCEPTION
//    WHEN OTHERS THEN
//      RAISE;
//    RETURN 1;
//    END TEST_SLEEP;

    final Async test = should.async();

    final JDBCPool pool = JDBCPool.pool(
      rule.vertx(),
      new JDBCConnectOptions()
        .setJdbcUrl("jdbc:oracle:thin:@127.0.0.1:1521:xe")
        .setUser("sys as sysdba")
        .setPassword("vertx"),
      new PoolOptions());

    pool.preparedQuery("SELECT TEST_SLEEP(10.5) FROM DUAL")
      .execute()
      .onFailure(should::fail)
      .onSuccess(rowSet -> {
        test.complete();
      });
  }
}
