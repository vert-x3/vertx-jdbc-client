package examples;

import io.vertx.core.Vertx;
import io.vertx.docgen.Source;
import io.vertx.ext.jdbc.spi.JDBCDecoder;
import io.vertx.ext.jdbc.spi.JDBCEncoder;
import io.vertx.ext.jdbc.spi.impl.JDBCEncoderImpl;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.jdbcclient.impl.actions.JDBCColumnDescriptor;

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


  public JDBCPool createJDBCPool(Vertx vertx, Class<JDBCEncoder> encoderClass, Class<JDBCDecoder> decoderClass) {
//    JsonObject extraOptions = new JsonObject()
//      .put("encoderCls", encoderClass.getName())
//      .put("decoderCls", decoderClass.getName());
//    JDBCConnectOptions options = new JDBCConnectOptions().setJdbcUrl("your_jdbc_url")
//      .setUser("your_database_user")
//      .setPassword("your_database_password");
//    PoolOptions poolOptions = new PoolOptions().setMaxSize(1);
//    DataSourceProvider provider = new AgroalCPDataSourceProvider(options, poolOptions).init(extraOptions);
//    return JDBCPool.pool(vertx, provider);
    return null; // TODO
  }
}
