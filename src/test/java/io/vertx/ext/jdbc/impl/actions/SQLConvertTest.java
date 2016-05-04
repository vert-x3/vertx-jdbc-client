package io.vertx.ext.jdbc.impl.actions;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class SQLConvertTest {

  @Parameterized.Parameters
  public static Collection<Object[]> generateData() {
    return Arrays.asList(new Object[][] {
        // simple types
        {"16:00:00"},
        {"2016-03-16"},
        {"2016-03-16T16:00:00Z"},
        {"f47ac10b-58cc-4372-a567-0e02b2c3d479"},
    });
  }

  // Fields
  private String value;

  public SQLConvertTest(String value) {
    this.value = value;
  }

  @Test
  public void testSQLConvert() throws SQLException {
    Object cast = JDBCStatementHelper.optimisticCast(value);
    Object convert = JDBCStatementHelper.convertSqlValue(cast);

    assertEquals(value, convert);
  }
}