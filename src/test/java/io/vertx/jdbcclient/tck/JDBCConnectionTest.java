package io.vertx.jdbcclient.tck;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.impl.AgroalCPDataSourceProvider;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.sqlclient.spi.DatabaseMetadata;
import io.vertx.sqlclient.tck.ConnectionTestBase;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;

@Ignore
@RunWith(VertxUnitRunner.class)
public class JDBCConnectionTest extends ConnectionTestBase {

  @Override
  public void setUp() throws Exception {
    super.setUp();

    Connection conn = DriverManager.getConnection("jdbc:h2:mem:test?shutdown=true");
    conn.createStatement()
      .execute(vertx.fileSystem().readFileBlocking("init.sql").toString());

    options = new JDBCConnectOptions()
      .setJdbcUrl("jdbc:h2:mem:test?shutdown=true")
      .setUser("")
      .setPassword("");

    connector = ClientConfig.POOLED.connect(vertx, options);
  }

  @Override
  public void tearDown(TestContext ctx) {
    super.tearDown(ctx);
  }

  @Override
  @Ignore("JDBC doesn't support metadata")
  protected void validateDatabaseMetaData(TestContext testContext, DatabaseMetadata databaseMetadata) {
  }
}
