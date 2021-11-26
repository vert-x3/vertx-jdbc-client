package io.vertx.it;

import io.vertx.ext.jdbc.impl.actions.SQLValueProvider;
import io.vertx.ext.jdbc.spi.impl.JDBCDecoderImpl;
import io.vertx.jdbcclient.impl.actions.JDBCColumnDescriptor;
import microsoft.sql.DateTimeOffset;
import microsoft.sql.Types;

import java.sql.SQLException;

public class MSSQLDecoder extends JDBCDecoderImpl {

  @Override
  protected Object decodeSpecificVendorType(SQLValueProvider valueProvider, JDBCColumnDescriptor descriptor) throws SQLException {
    final Object value = valueProvider.apply(null);
    if (value == null) {
      return null;
    }
    if (!descriptor.jdbcTypeWrapper().isSpecificVendorType()) {
      return value.toString();
    }
    if (descriptor.jdbcTypeWrapper().vendorTypeNumber() == Types.DATETIMEOFFSET) {
      if (value instanceof DateTimeOffset) {
        return ((DateTimeOffset) value).getOffsetDateTime();
      }
    }
    return value.toString();
  }

}
