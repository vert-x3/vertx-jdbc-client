package io.vertx.ext.jdbc;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
@DataObject
public class UpdateResult {

  private int updated;
  private JsonArray keys;

  public UpdateResult() {
  }

  public UpdateResult(UpdateResult other) {
    this.updated = other.updated;
    this.keys = other.getKeys();
  }

  @SuppressWarnings("unchecked")
  public UpdateResult(JsonObject json) {
    this.updated = json.getInteger("updated");
    keys = json.getJsonArray("keys");
  }

  public UpdateResult(int updated, JsonArray keys) {
    this.updated = updated;
    this.keys = keys;
  }

  public JsonObject toJson() {
    JsonObject obj = new JsonObject();
    obj.put("updated", updated);
    obj.put("keys", keys);
    return obj;
  }

  public int getUpdated() {
    return updated;
  }

  public JsonArray getKeys() {
    return keys;
  }
}
