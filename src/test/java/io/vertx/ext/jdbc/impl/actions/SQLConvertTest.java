package io.vertx.ext.jdbc.impl.actions;

import io.vertx.core.json.JsonObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static java.time.format.DateTimeFormatter.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class SQLConvertTest {

  private JsonObject config = new JsonObject().put("castUUID", true);

  private JDBCStatementHelper helper = new JDBCStatementHelper(config);

  @Parameters(name = "{0}")
  public static Collection<Object[]> generateData() {
    List<Object[]> params = new ArrayList<>();

    ZonedDateTime dateTime = ZonedDateTime.of(2016, 3, 16, 16, 0, 0, 0, ZoneId.of("Europe/Paris"));
    for (int i = 0; i < 4; i++) {
      int nanos = 123 * (i == 0 ? 0 : 1) * (int) Math.pow(1000, i > 1 ? i - 1 : 0);
      params.add(new Object[]{ISO_INSTANT.format(dateTime.withNano(nanos)), java.sql.Timestamp.class});
    }
    params.add(new Object[]{ISO_LOCAL_TIME.format(dateTime.withSecond(1)), java.sql.Time.class});
    params.add(new Object[]{dateTime.toLocalDate().toString(), java.sql.Date.class});

    params.add(new Object[]{"f47ac10b-58cc-4372-a567-0e02b2c3d479", UUID.class});

    return params;
  }

  private String value;
  private Class<?> expectedSqlType;

  public SQLConvertTest(String value, Class<?> expectedSqlType) {
    this.value = value;
    this.expectedSqlType = expectedSqlType;
  }

  @Test
  public void testSQLConvert() throws SQLException {
    Object cast = helper.optimisticCast(value);
    assertThat(cast, instanceOf(expectedSqlType));

    Object convert = JDBCStatementHelper.convertSqlValue(cast);
    assertEquals(value, convert);
  }
}
