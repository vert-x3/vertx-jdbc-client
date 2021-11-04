package io.vertx.ext.jdbc.impl.actions;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.sql.*;

@FunctionalInterface
public interface JDBCTypeProvider {

  Logger LOG = LoggerFactory.getLogger(JDBCTypeProvider.class);

  /**
   * Create provider by parameter metadata
   *
   * @param statement the prepared statement
   * @return a new {@code JDBCTypeProvider} instance
   * @see java.sql.ParameterMetaData
   */
  static JDBCTypeProvider fromParameter(PreparedStatement statement) {
    return col -> {
      try {
        return find(statement.getParameterMetaData().getParameterType(col));
      } catch (SQLFeatureNotSupportedException e) {
        LOG.warn("ParameterMetadata is unsupported by" + statement.getClass().getName(), e);
        return JDBCType.OTHER;
      }
    };
  }

  static JDBCType find(int vendorTypeNumber) {
    for (JDBCType jdbcType : JDBCType.values()) {
      if (jdbcType.getVendorTypeNumber() == vendorTypeNumber) {
        return jdbcType;
      }
    }
    return JDBCType.OTHER;
  }

  /**
   * Create provider by result set metadata
   *
   * @param rs the result set
   * @return a new {@code JDBCTypeProvider} instance
   * @see java.sql.ResultSetMetaData
   */
  static JDBCTypeProvider fromResult(ResultSet rs) {
    return col -> {
      try {
        return find(rs.getMetaData().getColumnType(col));
      } catch (SQLFeatureNotSupportedException e) {
        LOG.warn("ResultSetMetadata is unsupported by" + rs.getClass().getName(), e);
        return JDBCType.OTHER;
      }
    };
  }

  JDBCType apply(int sqlType) throws SQLException;

}
