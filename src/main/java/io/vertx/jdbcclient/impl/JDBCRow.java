package io.vertx.jdbcclient.impl;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.data.Numeric;
import io.vertx.sqlclient.impl.ArrayTuple;
import io.vertx.sqlclient.impl.RowDesc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;
import java.util.UUID;

public class JDBCRow extends ArrayTuple implements Row {

  private final RowDesc desc;

  public JDBCRow(RowDesc desc) {
    super(desc.columnNames().size());
    this.desc = desc;
  }

  public JDBCRow(JDBCRow row) {
    super(row);
    this.desc = row.desc;
  }

  @Override
  public String getColumnName(int pos) {
    List<String> columnNames = desc.columnNames();
    return pos < 0 || columnNames.size() - 1 < pos ? null : columnNames.get(pos);
  }

  @Override
  public int getColumnIndex(String name) {
    if (name == null) {
      throw new NullPointerException();
    }
    return desc.columnNames().indexOf(name);
  }

  @Override
  public <T> T get(Class<T> type, int pos) {
    if (type == Boolean.class) {
      return type.cast(getBoolean(pos));
    } else if (type == Short.class) {
      return type.cast(getShort(pos));
    } else if (type == Integer.class) {
      return type.cast(getInteger(pos));
    } else if (type == Long.class) {
      return type.cast(getLong(pos));
    } else if (type == Float.class) {
      return type.cast(getFloat(pos));
    } else if (type == Double.class) {
      return type.cast(getDouble(pos));
    } else if (type == Character.class) {
      return type.cast(getChar(pos));
    } else if (type == Numeric.class) {
      return type.cast(getNumeric(pos));
    } else if (type == String.class) {
      return type.cast(getString(pos));
    } else if (type == Buffer.class) {
      return type.cast(getBuffer(pos));
    } else if (type == UUID.class) {
      return type.cast(getUUID(pos));
    } else if (type == LocalDate.class) {
      return type.cast(getLocalDate(pos));
    } else if (type == LocalTime.class) {
      return type.cast(getLocalTime(pos));
    } else if (type == OffsetTime.class) {
      return type.cast(getOffsetTime(pos));
    } else if (type == LocalDateTime.class) {
      return type.cast(getLocalDateTime(pos));
    } else if (type == OffsetDateTime.class) {
      return type.cast(getOffsetDateTime(pos));
    } else if (type == JsonObject.class) {
      return type.cast(getJson(pos));
    } else if (type == JsonArray.class) {
      return type.cast(getJson(pos));
    } else if (type == Object.class) {
      return type.cast(getValue(pos));
    }
    throw new UnsupportedOperationException("Unsupported type " + type.getName());
  }

  @Override
  public <T> T[] getValues(Class<T> type, int pos) {
    if (type == Boolean.class) {
      return (T[]) getBooleanArray(pos);
    } else if (type == Short.class) {
      return (T[]) getShortArray(pos);
    } else if (type == Integer.class) {
      return (T[]) getIntegerArray(pos);
    } else if (type == Long.class) {
      return (T[]) getLongArray(pos);
    } else if (type == Float.class) {
      return (T[]) getFloatArray(pos);
    } else if (type == Double.class) {
      return (T[]) getDoubleArray(pos);
    } else if (type == Character.class) {
      return (T[]) getCharArray(pos);
    } else if (type == String.class) {
      return (T[]) getStringArray(pos);
    } else if (type == Buffer.class) {
      return (T[]) getBufferArray(pos);
    } else if (type == UUID.class) {
      return (T[]) getUUIDArray(pos);
    } else if (type == LocalDate.class) {
      return (T[]) getLocalDateArray(pos);
    } else if (type == LocalTime.class) {
      return (T[]) getLocalTimeArray(pos);
    } else if (type == OffsetTime.class) {
      return (T[]) getOffsetTimeArray(pos);
    } else if (type == LocalDateTime.class) {
      return (T[]) getLocalDateTimeArray(pos);
    } else if (type == OffsetDateTime.class) {
      return (T[]) getOffsetDateTimeArray(pos);
    } else if (type == Object.class) {
      return (T[]) getJsonArray(pos);
    }
    throw new UnsupportedOperationException("Unsupported type " + type.getName());
  }

  public Numeric getNumeric(String name) {
    int pos = desc.columnIndex(name);
    return pos == -1 ? null : getNumeric(pos);
  }

  public Object[] getJsonArray(String name) {
    int pos = desc.columnIndex(name);
    return pos == -1 ? null : getJsonArray(pos);
  }

  public Numeric[] getNumericArray(String name) {
    int pos = desc.columnIndex(name);
    return pos == -1 ? null : getNumericArray(pos);
  }

  public Character[] getCharArray(String name) {
    int pos = desc.columnIndex(name);
    return pos == -1 ? null : getCharArray(pos);
  }

  public Character getChar(int pos) {
    Object val = getValue(pos);
    if (val instanceof Character) {
      return (Character) val;
    } else {
      return null;
    }
  }

  public Numeric getNumeric(int pos) {
    Object val = getValue(pos);
    if (val instanceof Numeric) {
      return (Numeric) val;
    } else if (val instanceof Number) {
      return Numeric.parse(val.toString());
    }
    return null;
  }

  /**
   * Get a {@link io.vertx.core.json.JsonObject} or {@link io.vertx.core.json.JsonArray} value.
   */
  public Object getJson(int pos) {
    Object val = getValue(pos);
    if (val instanceof JsonObject) {
      return val;
    } else if (val instanceof JsonArray) {
      return val;
    } else {
      return null;
    }
  }

  public Character[] getCharArray(int pos) {
    Object val = getValue(pos);
    if (val instanceof Character[]) {
      return (Character[]) val;
    } else {
      return null;
    }
  }

  /**
   * Get a {@code Json} array value, the {@code Json} value may be a string, number, JSON object, array, boolean or null.
   */
  public Object[] getJsonArray(int pos) {
    Object val = getValue(pos);
    if (val instanceof Object[]) {
      return (Object[]) val;
    } else {
      return null;
    }
  }

  public Numeric[] getNumericArray(int pos) {
    Object val = getValue(pos);
    if (val instanceof Numeric[]) {
      return (Numeric[]) val;
    } else {
      return null;
    }
  }
}
