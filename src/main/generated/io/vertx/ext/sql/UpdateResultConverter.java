package io.vertx.ext.sql;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.ext.sql.UpdateResult}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.ext.sql.UpdateResult} original class using Vert.x codegen.
 */
public class UpdateResultConverter {


  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, UpdateResult obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "keys":
          if (member.getValue() instanceof JsonArray) {
            obj.setKeys(((JsonArray)member.getValue()).copy());
          }
          break;
        case "updated":
          if (member.getValue() instanceof Number) {
            obj.setUpdated(((Number)member.getValue()).intValue());
          }
          break;
      }
    }
  }

  public static void toJson(UpdateResult obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(UpdateResult obj, java.util.Map<String, Object> json) {
    if (obj.getKeys() != null) {
      json.put("keys", obj.getKeys());
    }
    json.put("updated", obj.getUpdated());
  }
}
