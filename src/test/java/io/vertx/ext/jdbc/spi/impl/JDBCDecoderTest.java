package io.vertx.ext.jdbc.spi.impl;

import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static java.time.format.DateTimeFormatter.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class JDBCDecoderTest {

  @Parameters(name = "{0}")
  public static Collection<Object[]> generateData() {
    List<Object[]> params = new ArrayList<>();

    ZonedDateTime dateTime = ZonedDateTime.of(2016, 3, 16, 16, 0, 0, 0, ZoneId.of("Europe/Paris"));
    for (int i = 0; i < 4; i++) {
      int nanos = 123 * (i == 0 ? 0 : 1) * (int) Math.pow(1000, i > 1 ? i - 1 : 0);
      params.add(new Object[]{ISO_INSTANT.format(dateTime.withNano(nanos)),
        dateTime.withNano(nanos).toOffsetDateTime().withOffsetSameInstant(ZoneOffset.UTC), OffsetDateTime.class, JDBCType.TIMESTAMP_WITH_TIMEZONE});
    }
    params.add(new Object[]{ISO_LOCAL_TIME.format(dateTime.withSecond(1)), dateTime.withSecond(1).toLocalTime(), LocalTime.class, JDBCType.TIME});
    params.add(new Object[]{dateTime.toLocalDate().toString(), dateTime.toLocalDate(), LocalDate.class, JDBCType.DATE});

    params.add(new Object[]{"f47ac10b-58cc-4372-a567-0e02b2c3d479", UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479"), UUID.class, JDBCType.BINARY});

    return params;
  }

  private JDBCStatementHelper helper = new JDBCStatementHelper();
  private String value;
  private Object expectedSqlValue;
  private Class<?> expectedSqlType;
  private JDBCType jdbcType;

  public JDBCDecoderTest(String value, Object expectedSqlValue, Class<?> expectedSqlType, JDBCType jdbcType) {
    this.value = value;
    this.expectedSqlValue = expectedSqlValue;
    this.expectedSqlType = expectedSqlType;
    this.jdbcType = jdbcType;
  }

  @Test
  public void testSQLConvert() throws SQLException {
    Object cast = ((JDBCEncoderImpl) helper.getEncoder()).encode(jdbcType, value);
    assertThat(cast, instanceOf(expectedSqlType));

    Object convert = helper.getDecoder().cast(cast);
    assertEquals(expectedSqlValue, convert);
  }
}
