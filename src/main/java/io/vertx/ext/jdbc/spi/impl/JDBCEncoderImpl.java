package io.vertx.ext.jdbc.spi.impl;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.ext.jdbc.spi.JDBCEncoder;

import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.TimeZone;

import static java.time.format.DateTimeFormatter.*;

public class JDBCEncoderImpl implements JDBCEncoder {

  private static final Logger log = LoggerFactory.getLogger(JDBCEncoder.class);

  private boolean castUUID;
  private boolean castDate;
  private boolean castTime;
  private boolean castDatetime;

  @Override
  public JDBCEncoder setup(boolean castUUID, boolean castDate, boolean castTime, boolean castDatetime) {
    this.castUUID = castUUID;
    this.castDate = castDate;
    this.castTime = castTime;
    this.castDatetime = castDatetime;
    return this;
  }

  @Override
  public Object convert(JDBCType jdbcType, Object javaValue) throws SQLException {
    if (javaValue == null) {
      return null;
    }
    if (javaValue instanceof String) {
      return optimisticCast((String) javaValue);
    }
    return javaValue;
  }

  protected Object optimisticCast(String value) {
    try {
      // sql time
      if (castTime && JDBCStatementHelper.TIME.matcher(value).matches()) {
        // convert from local time to instant
        Instant instant = LocalTime.parse(value).atDate(LocalDate.of(1970, 1, 1)).toInstant(ZoneOffset.UTC);
        // calculate the timezone offset in millis
        int offset = TimeZone.getDefault().getOffset(instant.toEpochMilli());
        // need to remove the offset since time has no TZ component
        return new Time(instant.toEpochMilli() - offset);
      }

      // sql date
      if (castDate && JDBCStatementHelper.DATE.matcher(value).matches()) {
        // convert from local date to instant
        Instant instant = LocalDate.parse(value).atTime(LocalTime.of(0, 0, 0, 0)).toInstant(ZoneOffset.UTC);
        // calculate the timezone offset in millis
        int offset = TimeZone.getDefault().getOffset(instant.toEpochMilli());
        // need to remove the offset since time has no TZ component
        return new Date(instant.toEpochMilli() - offset);
      }

      // sql timestamp
      if (castDatetime && JDBCStatementHelper.DATETIME.matcher(value).matches()) {
        Instant instant = Instant.from(ISO_INSTANT.parse(value));
        return Timestamp.from(instant);
      }

      // sql uuid
      if (castUUID && JDBCStatementHelper.UUID.matcher(value).matches()) {
        return java.util.UUID.fromString(value);
      }

    } catch (RuntimeException e) {
      log.debug(e);
    }

    // unknown
    return value;
  }
}
