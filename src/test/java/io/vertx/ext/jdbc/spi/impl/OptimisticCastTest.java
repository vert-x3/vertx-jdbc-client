package io.vertx.ext.jdbc.spi.impl;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class OptimisticCastTest {

  @Parameterized.Parameters
  public static Collection<Object[]> generateData() {
    return Arrays.asList(new Object[][]{
      // simple types
      {JDBCType.TIME, "16:00:00", LocalTime.class, null},
      {JDBCType.TIME, "16:00:00", String.class, new JsonObject().put("castTime", false)},
      {JDBCType.DATE, "2016-03-16", LocalDate.class, null},
      {JDBCType.DATE, "2016-03-16", String.class, new JsonObject().put("castDate", false)},
      {JDBCType.TIMESTAMP_WITH_TIMEZONE, "2016-03-16T16:00:00Z", OffsetDateTime.class, null},
      {JDBCType.TIMESTAMP, "2016-03-16T16:00:00Z", OffsetDateTime.class, null},
      {JDBCType.TIMESTAMP, "2016-03-16T16:00:00", LocalDateTime.class, null},
      {JDBCType.TIMESTAMP, "2016-03-16T16:00:00Z", String.class, new JsonObject().put("castDatetime", false)},
      {JDBCType.OTHER, "f47ac10b-58cc-4372-a567-0e02b2c3d479", UUID.class, new JsonObject().put("castUUID", true)},
      {JDBCType.OTHER, "f47ac10b-58cc-4372-a567-0e02b2c3d479", String.class, null},
      // bad variations
      {JDBCType.TIME, "24:00:00", LocalTime.class, null},
      {JDBCType.TIME, "2016-00-00", LocalTime.class, null},
    });
  }

  private final JDBCType jdbcType;
  private final JDBCEncoderImpl encoder;
  private final String value;
  private final Class expectedType;

  public OptimisticCastTest(JDBCType jdbcType, String value, Class expectedType, JsonObject config) {
    this.jdbcType = jdbcType;
    this.encoder = (JDBCEncoderImpl) (config == null ? new JDBCStatementHelper() : new JDBCStatementHelper(config)).getEncoder();
    this.value = value;
    this.expectedType = expectedType;
  }

  @Test
  public void testOptimisticCast() throws SQLException {
    assertEquals(value, expectedType, encoder.convert(jdbcType, value).getClass());
  }
}
