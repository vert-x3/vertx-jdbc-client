package io.vertx.ext.jdbc;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class TestDSProvider implements DataSourceProvider {

  public TestDSProvider() {
  }

  public static AtomicInteger instanceCount = new AtomicInteger();

  @Override
  public DataSource getDataSource(JsonObject config) throws SQLException {
    instanceCount.incrementAndGet();
    return new TestDS();
  }

  @Override
  public int maximumPoolSize(DataSource dataSource, JsonObject config) throws SQLException {
    return -1;
  }

  @Override
  public void close(DataSource dataSource) throws SQLException {
    if (dataSource instanceof TestDS) {
      instanceCount.decrementAndGet();
    }
  }

}
