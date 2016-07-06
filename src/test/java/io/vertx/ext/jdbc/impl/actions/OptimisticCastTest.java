package io.vertx.ext.jdbc.impl.actions;

import io.vertx.core.json.JsonObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class OptimisticCastTest {

  private JDBCStatementHelper helper = new JDBCStatementHelper(new JsonObject().put("castUUID", true));

  @Parameterized.Parameters
  public static Collection<Object[]> generateData() {
    return Arrays.asList(new Object[][]{
        // simple types
        {"16:00:00", "java.sql.Time"},
        {"2016-03-16", "java.sql.Date"},
        {"2016-03-16T16:00:00Z", "java.sql.Timestamp"},
        {"f47ac10b-58cc-4372-a567-0e02b2c3d479", "java.util.UUID"},
        // bad variations
        {"2016-03-16T16:00:00", "java.lang.String"},
        {"24:00:00", "java.lang.String"},
        {"2016-00-00", "java.lang.String"},
    });
  }

  // Fields
  private String value;
  private String expectedType;

  public OptimisticCastTest(String value, String expectedType) {
    this.value = value;
    this.expectedType = expectedType;
  }

  @Test
  public void testOptimisticCast() {
    assertEquals(value, expectedType, helper.optimisticCast(value).getClass().getName());
  }
}