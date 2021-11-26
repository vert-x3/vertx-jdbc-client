package io.vertx.jdbcclient.impl.actions;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

public interface JDBCPropertyAccessor<T> {

  Logger LOG = LoggerFactory.getLogger(JDBCColumnDescriptor.class);

  T get() throws SQLException;

  static <T> JDBCPropertyAccessor<T> create(JDBCPropertyAccessor<T> accessor) {
    return create(accessor, null);
  }

  static <T> JDBCPropertyAccessor<T> create(JDBCPropertyAccessor<T> accessor, T fallbackIfUnsupported) {
    return () -> {
      try {
        return accessor.get();
      } catch (SQLFeatureNotSupportedException e) {
        LOG.debug("Unsupported access properties in SQL metadata", e);
        return fallbackIfUnsupported;
      }
    };
  }

  static JDBCPropertyAccessor<Integer> jdbcType(JDBCPropertyAccessor<Integer> accessor) {
    return create(accessor, JDBCType.OTHER.getVendorTypeNumber());
  }

}
