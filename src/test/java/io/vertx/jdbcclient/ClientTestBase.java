package io.vertx.jdbcclient;

import io.vertx.ThreadLeakCheckerRule;
import io.vertx.core.Vertx;
import io.vertx.ext.jdbc.DBConfigs;
import io.vertx.ext.jdbc.JDBCClientTestBase;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;

public class ClientTestBase {

//  @Rule
//  public ThreadLeakCheckerRule leakRule = new ThreadLeakCheckerRule();

  protected Vertx vertx;
  protected Pool client;

  @Before
  public void setUp() throws Exception {
    JDBCClientTestBase.resetDb(ClientTestBase.class);
    vertx = Vertx.vertx();
    client = JDBCPool.pool(vertx, connectOptions(), poolOptions());
  }

  protected JDBCConnectOptions connectOptions() {
    return new JDBCConnectOptions()
      .setJdbcUrl(DBConfigs.hsqldb(ClientTestBase.class).getString("url"));
  }

  protected PoolOptions poolOptions() {
    return new PoolOptions().setMaxSize(1);
  }

  @After
  public void after(TestContext ctx) throws Exception {
    client.close().onComplete(ctx.asyncAssertSuccess(v -> {
      vertx.close().onComplete(ctx.asyncAssertSuccess());
    }));
  }
}
