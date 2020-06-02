package io.vertx.ext.sql.test;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class ResultSetTest {

  protected List<String> columnNames;
  protected List<JsonArray> results;
  protected ResultSet rs;
  protected int numRows = 10;

  @Before
  public void before() {

    columnNames = Arrays.asList("foo", "bar", "wibble");
    results = new ArrayList<>();
    int numRows = 10;
    for (int i = 0; i < numRows; i++) {
      JsonArray result = new JsonArray();
      for (int j = 0; j < columnNames.size(); j++) {
        result.add("res" + j);
      }
      results.add(result);
    }

    rs = new ResultSet(columnNames, results, null);
  }


  @Test
  public void testResultSet() {

    assertEquals(numRows, rs.getNumRows());
    assertEquals(columnNames.size(), rs.getNumColumns());
    assertEquals(columnNames.size(), rs.getColumnNames().size());
    assertEquals(columnNames, rs.getColumnNames());
    assertEquals(results, rs.getResults());

    List<JsonObject> rows = rs.getRows();
    assertEquals(numRows, rs.getRows().size());
    int index = 0;
    for (JsonObject row: rows) {
      JsonArray result = results.get(index);
      assertEquals(columnNames.size(), row.size());
      assertEquals(row.size(), result.size());
      for (int i = 0; i < columnNames.size(); i++) {
        String columnName = columnNames.get(i);
        String columnValue = result.getString(i);
        assertEquals(columnValue, row.getString(columnName));
      }
      index++;
    }

  }

  @Test
  public void testCaseInsensitiveRows() {
    JsonObject row = rs.getRows(true).get(0);
    assertEquals("res0", row.getString("foo"));
    assertEquals("res0", row.getString("FOO"));
    assertEquals("res0", row.getString("fOo"));
  }

  @Test
  public void testJson() {

    JsonObject json = rs.toJson();
    ResultSet rs2 = new ResultSet(json);
    assertEquals(rs, rs2);

  }
}
