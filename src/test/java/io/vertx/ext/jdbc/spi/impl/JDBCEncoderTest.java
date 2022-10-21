package io.vertx.ext.jdbc.spi.impl;

import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.jdbcclient.impl.actions.JDBCColumnDescriptor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.JDBCType;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class JDBCEncoderTest {

  static TimeZone aDefault;

  @BeforeClass
  public static void setup() {
    aDefault = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  @AfterClass
  public static void shutdown() {
    TimeZone.setDefault(aDefault);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> generateData() {
    return Arrays.asList(new Object[][]{
      {JDBCType.DATE, "2016-03-16", LocalDate.class, null},
      {JDBCType.TIME, "16:00:00", LocalTime.class, null},
      {JDBCType.TIME_WITH_TIMEZONE, "16:00:00+07:00", OffsetTime.class, null},
      {JDBCType.TIME, "16:00:00+07:00", java.sql.Time.class,
        new java.sql.Time(Instant.EPOCH.plus(9, ChronoUnit.HOURS).toEpochMilli())},
      {JDBCType.TIMESTAMP, "2016-03-16T16:00:00", LocalDateTime.class, null},
      {JDBCType.TIMESTAMP_WITH_TIMEZONE, "2016-03-16T16:00:00Z", OffsetDateTime.class,
        OffsetDateTime.of(LocalDateTime.of(2016, 3, 16, 16, 0, 0), ZoneOffset.UTC)},
      {JDBCType.TIMESTAMP, "2016-03-16T16:00:00+07:00", LocalDateTime.class,
        LocalDateTime.of(2016, 3, 16, 9, 0, 0)},
      {JDBCType.OTHER, "f47ac10b-58cc-4372-a567-0e02b2c3d479", UUID.class, null},
      {JDBCType.BINARY, "f47ac10b-58cc-4372-a567-0e02b2c3d479", UUID.class, null},
      {JDBCType.VARCHAR, "f47ac10b-58cc-4372-a567-0e02b2c3d479", String.class, null},
    });
  }

  private final JDBCType jdbcType;
  private final JDBCEncoderImpl encoder;
  private final String value;
  private final Class expectedType;
  private final Object expectedValue;

  public JDBCEncoderTest(JDBCType jdbcType, String value, Class expectedType, Object expectedValue) {
    this.jdbcType = jdbcType;
    this.encoder = (JDBCEncoderImpl) new JDBCStatementHelper().getEncoder();
    this.value = value;
    this.expectedType = expectedType;
    this.expectedValue = expectedValue;
  }

  @Test
  public void testEncoder() {
    final Object sqlValue = encoder.doEncode(JDBCColumnDescriptor.wrap(jdbcType), value);
    assertEquals(value, expectedType, sqlValue.getClass());
    if (Objects.nonNull(expectedValue)) {
      assertEquals(value, expectedValue, sqlValue);
    }
  }
}
