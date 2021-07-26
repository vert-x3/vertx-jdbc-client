package io.vertx.ext.jdbc.impl.actions;

import io.vertx.core.json.JsonObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class OptimisticCastTest {

  private JDBCStatementHelper helper;

  @Parameterized.Parameters
  public static Collection<Object[]> generateData() {
    return Arrays.asList(new Object[][]{
      // simple types
      {"16:00:00", "java.sql.Time", null},
      {"16:00:00", "java.lang.String", new JsonObject().put("castTime", false)},
      {"2016-03-16", "java.sql.Date", null},
      {"2016-03-16", "java.lang.String", new JsonObject().put("castDate", false)},
      {"2016-03-16T16:00:00Z", "java.sql.Timestamp", null},
      {"2016-03-16T16:00:00Z", "java.lang.String", new JsonObject().put("castDatetime", false)},
      {"f47ac10b-58cc-4372-a567-0e02b2c3d479", "java.util.UUID", new JsonObject().put("castUUID", true)},
      {"f47ac10b-58cc-4372-a567-0e02b2c3d479", "java.lang.String", null},
      // bad variations
      {"2016-03-16T16:00:00", "java.lang.String", null},
      {"24:00:00", "java.lang.String", null},
      {"2016-00-00", "java.lang.String", null},
    });
  }

  // Fields
  private String value;
  private String expectedType;

  public OptimisticCastTest(String value, String expectedType, JsonObject config) {
    this.helper = config == null ? new JDBCStatementHelper() : new JDBCStatementHelper(config);
    this.value = value;
    this.expectedType = expectedType;
  }

  @Test
  public void testOptimisticCast() throws SQLException {
    assertEquals(value, expectedType, helper.getEncoder().convert(JDBCType.OTHER, value).getClass().getName());
  }
}
