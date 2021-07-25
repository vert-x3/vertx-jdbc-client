package io.vertx.ext.jdbc.spi;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.function.Function;
import java.util.regex.Pattern;

public interface JDBCParser {

  Pattern DATETIME = Pattern.compile("^\\d{4}-(?:0[0-9]|1[0-2])-[0-9]{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3,9})?Z$");
  Pattern DATE = Pattern.compile("^\\d{4}-(?:0[0-9]|1[0-2])-[0-9]{2}$");
  Pattern TIME = Pattern.compile("^\\d{2}:\\d{2}:\\d{2}$");
  Pattern UUID = Pattern.compile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");

  Function<JDBCType, Class> LOOKUP_SQL_NUMBER = jdbcType -> {
    switch (jdbcType) {
      case TINYINT:
        return byte.class;
      case SMALLINT:
        return Short.class;
      case INTEGER:
        return Integer.class;
      case BIGINT:
        return Long.class;
      case FLOAT:
      case REAL:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case NUMERIC:
      case DECIMAL:
        return BigDecimal.class;
      default:
        throw new IllegalArgumentException("Invalid Number JDBC Type");
    }
  };

  Function<JDBCType, Class> LOOKUP_SQL_DATETIME = jdbcType -> {
    switch (jdbcType) {
      case DATE:
        return LocalDate.class;
      case TIME:
        return LocalTime.class;
      case TIMESTAMP:
        return LocalDateTime.class;
      case TIME_WITH_TIMEZONE:
        return OffsetTime.class;
      case TIMESTAMP_WITH_TIMEZONE:
        return OffsetDateTime.class;
      default:
        throw new IllegalArgumentException("Invalid Date Time JDBC Type");
    }
  };

}
