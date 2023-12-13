package io.vertx.ext.sql;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converter and mapper for {@link io.vertx.ext.sql.ResultSet}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.ext.sql.ResultSet} original class using Vert.x codegen.
 */
public class ResultSetConverter {


  private static final Base64.Decoder BASE64_DECODER = JsonUtil.BASE64_DECODER;
  private static final Base64.Encoder BASE64_ENCODER = JsonUtil.BASE64_ENCODER;

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ResultSet obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "columnNames":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<java.lang.String> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                list.add((String)item);
            });
            obj.setColumnNames(list);
          }
          break;
        case "next":
          if (member.getValue() instanceof JsonObject) {
            obj.setNext(new io.vertx.ext.sql.ResultSet((io.vertx.core.json.JsonObject)member.getValue()));
          }
          break;
        case "numColumns":
          break;
        case "numRows":
          break;
        case "output":
          if (member.getValue() instanceof JsonArray) {
            obj.setOutput(((JsonArray)member.getValue()).copy());
          }
          break;
        case "results":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<io.vertx.core.json.JsonArray> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof JsonArray)
                list.add(((JsonArray)item).copy());
            });
            obj.setResults(list);
          }
          break;
        case "rows":
          if (member.getValue() instanceof JsonArray) {
          }
          break;
      }
    }
  }

   static void toJson(ResultSet obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(ResultSet obj, java.util.Map<String, Object> json) {
    if (obj.getColumnNames() != null) {
      JsonArray array = new JsonArray();
      obj.getColumnNames().forEach(item -> array.add(item));
      json.put("columnNames", array);
    }
    if (obj.getNext() != null) {
      json.put("next", obj.getNext().toJson());
    }
    json.put("numColumns", obj.getNumColumns());
    json.put("numRows", obj.getNumRows());
    if (obj.getOutput() != null) {
      json.put("output", obj.getOutput());
    }
    if (obj.getResults() != null) {
      JsonArray array = new JsonArray();
      obj.getResults().forEach(item -> array.add(item));
      json.put("results", array);
    }
    if (obj.getRows() != null) {
      JsonArray array = new JsonArray();
      obj.getRows().forEach(item -> array.add(item));
      json.put("rows", array);
    }
  }
}
