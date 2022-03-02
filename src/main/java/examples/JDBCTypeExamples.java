package examples;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.docgen.Source;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.jdbc.spi.DataSourceProvider;
import io.vertx.ext.jdbc.spi.JDBCDecoder;
import io.vertx.ext.jdbc.spi.JDBCEncoder;
import io.vertx.ext.jdbc.spi.impl.JDBCEncoderImpl;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.jdbcclient.impl.AgroalCPDataSourceProvider;
import io.vertx.jdbcclient.impl.actions.JDBCColumnDescriptor;
import io.vertx.sqlclient.PoolOptions;

import java.sql.Date;
import java.sql.JDBCType;
import java.time.LocalDate;

@Source
public class JDBCTypeExamples {

  public static class DerbyEncoder extends JDBCEncoderImpl {
    @Override
    protected Object encodeDateTime(JDBCColumnDescriptor descriptor, Object value) {
      Object v = super.encodeDateTime(descriptor, value);
      if (descriptor.jdbcType() == JDBCType.DATE) {
        return Date.valueOf((LocalDate) v);
      }
      return v;
    }
  }

  public JDBCClient createJDBCClient(Vertx vertx, Class<JDBCEncoder> encoderClass, Class<JDBCDecoder> decoderClass) {
    JsonObject options = new JsonObject().put("url", "your_jdbc_url")
      .put("user", "your_database_user")
      .put("password", "your_database_password")
      .put("encoderCls", encoderClass.getName())
      .put("decoderCls", decoderClass.getName());
    return JDBCClient.createShared(vertx, options);
  }

  public JDBCPool createJDBCPool(Vertx vertx, Class<JDBCEncoder> encoderClass, Class<JDBCDecoder> decoderClass) {
    JsonObject extraOptions = new JsonObject()
      .put("encoderCls", encoderClass.getName())
      .put("decoderCls", decoderClass.getName());
    JDBCConnectOptions options = new JDBCConnectOptions().setJdbcUrl("your_jdbc_url")
      .setUser("your_database_user")
      .setPassword("your_database_password");
    PoolOptions poolOptions = new PoolOptions().setMaxSize(1);
    DataSourceProvider provider = new AgroalCPDataSourceProvider(options, poolOptions).init(extraOptions);
    return JDBCPool.pool(vertx, provider);
  }
}
