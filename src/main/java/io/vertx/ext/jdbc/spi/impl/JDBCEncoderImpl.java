package io.vertx.ext.jdbc.spi.impl;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.ext.jdbc.impl.actions.JDBCTypeProvider;
import io.vertx.ext.jdbc.spi.JDBCEncoder;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

public class JDBCEncoderImpl implements JDBCEncoder {

  private static final Logger log = LoggerFactory.getLogger(JDBCEncoder.class);

  @Override
  public Object encode(JsonArray input, int pos, JDBCTypeProvider provider) throws SQLException {
    return encode(provider.apply(pos), input.getValue(pos - 1));
  }

  protected Object encode(JDBCType jdbcType, Object javaValue) throws SQLException {
    if (javaValue == null) {
      return null;
    }
    if (isAbleToUUID(jdbcType) && javaValue instanceof String && JDBCStatementHelper.UUID.matcher((String) javaValue).matches()) {
      return debug(jdbcType, java.util.UUID.fromString((String) javaValue));
    }
    try {
      JDBCStatementHelper.LOOKUP_SQL_DATETIME.apply(jdbcType);
      return debug(jdbcType, castDateTime(jdbcType, javaValue));
    } catch (IllegalArgumentException e) {
      //ignore
    }
    return debug(jdbcType, javaValue);
  }

  protected boolean isAbleToUUID(JDBCType jdbcType) {
    return jdbcType == JDBCType.BINARY || jdbcType == JDBCType.VARBINARY || jdbcType == JDBCType.OTHER;
  }

  protected Object castDateTime(JDBCType jdbcType, Object value) {
    if (jdbcType == JDBCType.DATE) {
      if (value instanceof String) {
        return LocalDate.parse((String) value);
      }
      if (value instanceof java.util.Date) {
        return ((java.util.Date) value).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
      }
      return value;
    }
    if (jdbcType == JDBCType.TIME_WITH_TIMEZONE) {
      if (value instanceof String) {
        return OffsetTime.parse((String) value);
      }
      return value;
    }
    if (jdbcType == JDBCType.TIME) {
      if (value instanceof String) {
        try {
          return LocalTime.parse((String) value);
        } catch (DateTimeParseException e) {
          return OffsetTime.parse((String) value).withOffsetSameInstant(ZoneOffset.UTC).toLocalTime();
        }
      }
      return value;
    }
    if (jdbcType == JDBCType.TIMESTAMP_WITH_TIMEZONE) {
      if (value instanceof String) {
        return OffsetDateTime.parse((String) value);
      }
      return value;
    }
    if (jdbcType == JDBCType.TIMESTAMP) {
      if (value instanceof String) {
        try {
          return LocalDateTime.parse((String) value);
        } catch (DateTimeParseException e) {
          return OffsetDateTime.parse((String) value).withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
        }
      }
      return value;
    }
    throw new IllegalArgumentException("Invalid Date Time JDBC Type");
  }

  protected Object debug(JDBCType jdbcType, Object javaValue) {
    log.debug("Convert JDBC type [" + jdbcType + "][" + javaValue.getClass().getName() + "]");
    return javaValue;
  }

}
