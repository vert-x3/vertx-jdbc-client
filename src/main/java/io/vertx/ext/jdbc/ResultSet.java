package io.vertx.ext.jdbc;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
@DataObject
public class ResultSet {

  private List<String> columnNames;
  private List<JsonArray> results;

  public ResultSet() {
  }

  public ResultSet(ResultSet other) {
    this.columnNames = other.columnNames;
    this.results = other.results;
  }

  public ResultSet(List<String> columnNames, List<JsonArray> results) {
    this.columnNames = columnNames;
    this.results = results;
  }

  @SuppressWarnings("unchecked")
  public ResultSet(JsonObject json) {
    JsonArray arr = json.getJsonArray("columnNames");
    if (arr != null) {
      this.columnNames = arr.getList();
    }
    arr = json.getJsonArray("results");
    if (arr != null) {
      results = arr.getList();
    }
  }

  public JsonObject toJson() {
    JsonObject obj = new JsonObject();
    obj.put("columnNames", new JsonArray(columnNames));
    obj.put("results", new JsonArray(results));
    return obj;
  }

  public List<JsonArray> getResults() {
    return results;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

}
