package io.vertx.ext.jdbc.spi.impl;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.ext.jdbc.spi.JDBCColumnDescriptorProvider;
import io.vertx.ext.jdbc.spi.JDBCEncoder;
import io.vertx.jdbcclient.impl.actions.JDBCColumnDescriptor;

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
import java.util.UUID;

public class JDBCEncoderImpl implements JDBCEncoder {

  private static final Logger LOG = LoggerFactory.getLogger(JDBCEncoder.class);

  @Override
  public Object encode(JsonArray input, int pos, JDBCColumnDescriptorProvider provider) throws SQLException {
    return doEncode(provider.apply(pos), input.getValue(pos - 1));
  }

  protected Object doEncode(JDBCColumnDescriptor descriptor, Object javaValue) {
    if (javaValue == null) {
      return null;
    }
    if (descriptor.jdbcTypeWrapper().isDateTimeType()) {
      return debug(descriptor, encodeDateTime(descriptor, javaValue));
    }
    if (descriptor.jdbcTypeWrapper().isSpecificVendorType()) {
      return debug(descriptor, encodeSpecificVendorType(descriptor, javaValue));
    }
    return debug(descriptor, encodeData(descriptor, javaValue));
  }

  /**
   * Convert the parameter {@code Java datetime} value to the {@code SQL datetime} value
   *
   * @param descriptor the column descriptor
   * @param value      the java value in parameter
   * @return the compatible SQL value
   */
  protected Object encodeDateTime(JDBCColumnDescriptor descriptor, Object value) {
    JDBCType jdbcType = descriptor.jdbcType();
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

  /**
   * Convert the parameter {@code Java} value to the {@code specific SQL vendor data type}
   *
   * @param descriptor the column descriptor
   * @param javaValue  the java value in parameter
   * @return the compatible SQL value
   */
  protected Object encodeSpecificVendorType(JDBCColumnDescriptor descriptor, Object javaValue) {
    return javaValue;
  }

  /**
   * Convert any the parameter {@code Java} value expect {@link #encodeDateTime(JDBCColumnDescriptor, Object)} and
   * {@link #encodeSpecificVendorType(JDBCColumnDescriptor, Object)} to the {@code SQL value}
   *
   * @param descriptor the column descriptor
   * @param javaValue  the java value in parameter
   * @return the compatible SQL value
   */
  protected Object encodeData(JDBCColumnDescriptor descriptor, Object javaValue) {
    if (descriptor.jdbcTypeWrapper().isAbleAsUUID() && javaValue instanceof String && JDBCStatementHelper.UUID.matcher((String) javaValue).matches()) {
      return UUID.fromString((String) javaValue);
    }
    return javaValue;
  }

  private Object debug(JDBCColumnDescriptor descriptor, Object javaValue) {
    LOG.debug("Convert JDBC column [" + descriptor + "][" + javaValue.getClass().getName() + "]");
    return javaValue;
  }

}
