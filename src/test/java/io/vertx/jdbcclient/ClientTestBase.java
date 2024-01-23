package io.vertx.jdbcclient;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.junit.After;
import org.junit.Before;

public abstract class ClientTestBase {

//  @Rule
//  public ThreadLeakCheckerRule leakRule = new ThreadLeakCheckerRule();

  protected Vertx vertx;
  protected Pool client;

  @Before
  public void setUp() throws Exception {
    vertx = Vertx.vertx();
    client = JDBCPool.pool(vertx, connectOptions(), poolOptions());
  }

  protected JDBCConnectOptions connectOptions() {
    return DataSourceConfigs.hsqldb(getClass());
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
