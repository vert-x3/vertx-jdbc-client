package io.vertx.jdbcclient.tck;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.tck.DriverTestBase;
import org.junit.Ignore;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class JDBCDriverTest extends DriverTestBase {

  @Override
  protected SqlConnectOptions defaultOptions() {
    return new JDBCConnectOptions()
      .setJdbcUrl("jdbc:h2:mem:test?shutdown=true")
      .setUser("")
      .setPassword("");
  }

  @Override
  @Ignore("JDBC required a jdbc connection string which isn't present in SqlConnectOptions")
  public void testCreatePoolFromDriver02(TestContext ctx) {
  }

  @Override
  @Ignore("JDBC required a jdbc connection string which isn't present in SqlConnectOptions")
  public void testCreatePool02(TestContext ctx) {
  }

  @Override
  @Ignore("JDBC required a jdbc connection string which isn't present in SqlConnectOptions")
  public void testCreatePool05(TestContext ctx) {
  }
}
