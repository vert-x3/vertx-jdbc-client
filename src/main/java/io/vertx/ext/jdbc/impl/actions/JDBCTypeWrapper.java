package io.vertx.ext.jdbc.impl.actions;

import io.vertx.codegen.annotations.Nullable;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.JDBCType;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLXML;
import java.sql.Struct;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class JDBCTypeWrapper {

  private static final Map<JDBCType, Class> SQL_NUMBER = Collections.unmodifiableMap(initNumberMapping());
  private static final Map<JDBCType, Class> SQL_DATETIME = Collections.unmodifiableMap(initDateTimeMapping());
  private static final Map<JDBCType, Class> SQL_STRING = Collections.unmodifiableMap(initStringMapping());
  private static final Map<JDBCType, Class> SQL_OTHER = Collections.unmodifiableMap(initOtherMapping());

  private final int vendorTypeNumber;
  private final String vendorTypeName;
  private final Class vendorTypeClass;
  private final JDBCType jdbcType;

  private JDBCTypeWrapper(int vendorTypeNumber, String vendorTypeName, Class vendorTypeClass, JDBCType jdbcType) {
    this.vendorTypeNumber = vendorTypeNumber;
    this.vendorTypeName = vendorTypeName;
    this.vendorTypeClass = vendorTypeClass;
    this.jdbcType = jdbcType;
  }

  public static JDBCTypeWrapper of(int vendorTypeNumber, String vendorTypeName, String vendorClassName) {
    final JDBCType jdbcType = Arrays.stream(JDBCType.values())
      .filter(n -> n.getVendorTypeNumber() == vendorTypeNumber)
      .findFirst()
      .orElse(null);
    return new JDBCTypeWrapper(vendorTypeNumber, vendorTypeName,
      vendorClassName == null ? null : loadVendorTypeClass(vendorClassName), jdbcType);
  }

  public static JDBCTypeWrapper of(JDBCType jdbcType) {
    return new JDBCTypeWrapper(jdbcType.getVendorTypeNumber(), null, null, jdbcType);
  }

  public int vendorTypeNumber() {
    return vendorTypeNumber;
  }

  @Nullable
  public String vendorTypeName() {
    return vendorTypeName;
  }

  @Nullable
  public Class vendorTypeClass() {
    if (isSpecificVendorType()) {
      return vendorTypeClass;
    }
    if (isDateTimeType()) {
      return SQL_DATETIME.get(this.jdbcType);
    }
    if (isNumberType()) {
      return SQL_NUMBER.get(this.jdbcType);
    }
    if (isStringType()) {
      return SQL_STRING.get(this.jdbcType);
    }
    return SQL_OTHER.get(this.jdbcType);
  }

  /**
   * @return the most appropriate {@code JDBCType} or {@code null} if it is advanced type of specific database
   */
  @Nullable
  public JDBCType jdbcType() {
    return jdbcType;
  }

  public boolean isSpecificVendorType() {
    return this.jdbcType == null;
  }

  public boolean isDateTimeType() {
    return SQL_DATETIME.containsKey(this.jdbcType);
  }

  public boolean isNumberType() {
    return SQL_NUMBER.containsKey(this.jdbcType);
  }

  public boolean isStringType() {
    return SQL_STRING.containsKey(this.jdbcType);
  }

  public boolean isBinaryType() {
    return this.jdbcType == JDBCType.BINARY || this.jdbcType == JDBCType.VARBINARY ||
      this.jdbcType == JDBCType.LONGVARBINARY;
  }

  public boolean isAbleAsUUID() {
    return jdbcType == JDBCType.BINARY || jdbcType == JDBCType.VARBINARY || jdbcType == JDBCType.OTHER;
  }

  /**
   * Check whether JDBCType is not implemented in Vertx
   *
   * @return true
   */
  public boolean isUnhandledType() {
    return Stream.of(JDBCType.NULL, JDBCType.OTHER, JDBCType.DISTINCT, JDBCType.REF_CURSOR, JDBCType.JAVA_OBJECT)
      .anyMatch(t -> t == this.jdbcType);
  }

  @Override
  public String toString() {
    return "JDBCTypeWrapper[vendorTypeNumber=(" + vendorTypeNumber + "), vendorTypeName=(" + vendorTypeName + "), " +
      "vendorTypeClass=(" + vendorTypeClass + "), jdbcType=(" + jdbcType + ")]";
  }

  private static EnumMap<JDBCType, Class> initNumberMapping() {
    final EnumMap<JDBCType, Class> map = new EnumMap<>(JDBCType.class);
    map.put(JDBCType.TINYINT, byte.class);
    map.put(JDBCType.SMALLINT, Short.class);
    map.put(JDBCType.INTEGER, Integer.class);
    map.put(JDBCType.BIGINT, Long.class);
    map.put(JDBCType.FLOAT, Float.class);
    map.put(JDBCType.REAL, Float.class);
    map.put(JDBCType.DOUBLE, Double.class);
    map.put(JDBCType.NUMERIC, BigDecimal.class);
    map.put(JDBCType.DECIMAL, BigDecimal.class);
    return map;
  }

  private static EnumMap<JDBCType, Class> initDateTimeMapping() {
    final EnumMap<JDBCType, Class> map = new EnumMap<>(JDBCType.class);
    map.put(JDBCType.DATE, LocalDate.class);
    map.put(JDBCType.TIME, LocalTime.class);
    map.put(JDBCType.TIMESTAMP, LocalDateTime.class);
    map.put(JDBCType.TIME_WITH_TIMEZONE, OffsetTime.class);
    map.put(JDBCType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.class);
    return map;
  }

  private static Map<JDBCType, Class> initStringMapping() {
    return Stream.of(JDBCType.CHAR, JDBCType.VARCHAR, JDBCType.LONGVARCHAR, JDBCType.NCHAR, JDBCType.NVARCHAR,
      JDBCType.LONGNVARCHAR).collect(Collectors.toMap(Function.identity(), o -> String.class));
  }

  private static Map<JDBCType, Class> initOtherMapping() {
    final EnumMap<JDBCType, Class> map = new EnumMap<>(JDBCType.class);
    map.put(JDBCType.ARRAY, Array.class);

    map.put(JDBCType.BINARY, byte[].class);
    map.put(JDBCType.VARBINARY, byte[].class);
    map.put(JDBCType.LONGVARBINARY, byte[].class);

    map.put(JDBCType.BIT, Boolean.class);
    map.put(JDBCType.BOOLEAN, Boolean.class);

    map.put(JDBCType.BLOB, Blob.class);

    map.put(JDBCType.CLOB, Clob.class);
    map.put(JDBCType.NCLOB, Clob.class);

    map.put(JDBCType.DATALINK, URL.class);
    map.put(JDBCType.REF, Ref.class);
    map.put(JDBCType.ROWID, RowId.class);
    map.put(JDBCType.SQLXML, SQLXML.class);
    map.put(JDBCType.STRUCT, Struct.class);
    Stream.of(JDBCType.NULL, JDBCType.OTHER, JDBCType.DISTINCT, JDBCType.REF_CURSOR, JDBCType.JAVA_OBJECT)
      .forEach(t -> map.put(t, null));
    return map;
  }

  private static Class loadVendorTypeClass(String vendorTypeClassName) {
    try {
      return Class.forName(vendorTypeClassName);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

}
