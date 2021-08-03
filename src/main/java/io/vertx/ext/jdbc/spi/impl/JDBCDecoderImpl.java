package io.vertx.ext.jdbc.spi.impl;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.ext.jdbc.spi.JDBCDecoder;
import io.vertx.sqlclient.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public class JDBCDecoderImpl implements JDBCDecoder {

  private static final Logger log = LoggerFactory.getLogger(JDBCDecoder.class);

  @Override
  public Object parse(ResultSetMetaData metaData, int pos, ResultSet rs) throws SQLException {
    return convert(JDBCType.valueOf(metaData.getColumnType(pos)), cls -> cls == null ? rs.getObject(pos) : rs.getObject(pos, cls));
  }

  @Override
  public Object parse(ParameterMetaData metaData, int pos, CallableStatement cs) throws SQLException {
    return convert(JDBCType.valueOf(metaData.getParameterType(pos)), cls -> cls == null ? cs.getObject(pos) : cs.getObject(pos, cls));
  }

  @Override
  public Object convert(JDBCType jdbcType, SQLValueProvider valueProvider) throws SQLException {
    switch (jdbcType) {
      case ARRAY:
        return cast(getCoerceObject(valueProvider, Array.class));
      case BLOB:
        return cast(getCoerceObject(valueProvider, Blob.class));
      case CLOB:
      case NCLOB:
        return cast(getCoerceObject(valueProvider, Clob.class));
      case BIT:
      case BOOLEAN:
        return cast(getCoerceObject(valueProvider, Boolean.class));
      case CHAR:
      case VARCHAR:
      case LONGVARCHAR:
      case NCHAR:
      case NVARCHAR:
      case LONGNVARCHAR:
        return cast(getCoerceObject(valueProvider, String.class));
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case REAL:
      case DOUBLE:
      case NUMERIC:
      case DECIMAL:
        return convertNumber(valueProvider, jdbcType);
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIME_WITH_TIMEZONE:
      case TIMESTAMP_WITH_TIMEZONE:
        return convertDateTime(valueProvider, jdbcType);
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
        return convertBinary(valueProvider, jdbcType);
      case DATALINK:
        return convertLink(valueProvider);
      case ROWID:
        return cast(getCoerceObject(valueProvider, RowId.class));
      case REF:
        return cast(getCoerceObject(valueProvider, Ref.class));
      case SQLXML:
        return convertXML(valueProvider);
      case STRUCT:
        return convertStruct(valueProvider);
      case NULL:
      case OTHER:
      case DISTINCT:
      case REF_CURSOR:
      case JAVA_OBJECT:
        log.debug("Fallback to string when handle JDBCType " + jdbcType);
        break;
    }
    return convertSpecialType(valueProvider, jdbcType);
  }

  @Override
  public Object cast(Object value) throws SQLException {
    if (value == null) {
      return null;
    }

    if (value instanceof Array) {
      return convertArray((Array) value);
    }

    if (value instanceof Blob) {
      Blob v = (Blob) value;
      return v.length() == 0L ? Buffer.buffer(0) : streamToBuffer(v.getBinaryStream(), Blob.class);
    }

    if (value instanceof Clob) {
      Clob v = (Clob) value;
      return v.length() == 0L ? "" : streamToBuffer(v.getAsciiStream(), Clob.class).toString();
    }

    if (value instanceof Ref) {
      return cast(((Ref) value).getObject());
    }

    // RowId
    if (value instanceof RowId) {
      return ((RowId) value).getBytes();
    }

    // Struct
    if (value instanceof Struct) {
      return Tuple.of(((Struct) value).getAttributes());
    }

    if (value instanceof Date) {
      return ((Date) value).toLocalDate();
    }

    if (value instanceof Time) {
      return ((Time) value).toLocalTime();
    }

    if (value instanceof Timestamp) {
      return ((Timestamp) value).toLocalDateTime();
    }

    return value;
  }

  protected Object convertArray(Array value) throws SQLException {
    JDBCType baseType = JDBCType.valueOf(value.getBaseType());
    try {
      Object arr = value.getArray();
      if (arr != null) {
        int len = java.lang.reflect.Array.getLength(arr);
        Object[] castedArray = new Object[len];
        for (int i = 0; i < len; i++) {
          int index = i;
          castedArray[i] = convert(baseType, cls -> java.lang.reflect.Array.get(arr, index));
        }
        return castedArray;
      }
      return value;
    } finally {
      value.free();
    }
  }

  /**
   * Convert a value from date time JDBCType to Java date time
   *
   * @see JDBCStatementHelper#LOOKUP_SQL_DATETIME
   */
  protected Object convertDateTime(SQLValueProvider valueProvider, JDBCType jdbcType) throws SQLException {
    try {
      return cast(valueProvider.apply(JDBCStatementHelper.LOOKUP_SQL_DATETIME.apply(jdbcType)));
    } catch (SQLException e) {
      log.debug("Error when convert SQL date time. Try coerce value", e);
      Object value = valueProvider.apply(null);
      if (value == null) {
        return null;
      }
      try {
        // Some JDBC drivers (PG driver) treats Timestamp with TimeZone/Time with TimeZone
        // to java.sql.timestamp/java.sql.time/String at UTC
        // and handles date time data type internally
        // then this code will try parse to OffsetTime/OffsetDateTime
        if (value instanceof Time) {
          return ((Time) value).toLocalTime().atOffset(ZoneOffset.UTC);
        }
        if (jdbcType == JDBCType.TIME || jdbcType == JDBCType.TIME_WITH_TIMEZONE) {
          return LocalTime.parse(value.toString(), DateTimeFormatter.ISO_LOCAL_TIME).atOffset(ZoneOffset.UTC);
        }
        if (value instanceof Timestamp) {
          return ((Timestamp) value).toLocalDateTime().atOffset(ZoneOffset.UTC);
        }
        if (jdbcType == JDBCType.TIMESTAMP || jdbcType == JDBCType.TIMESTAMP_WITH_TIMEZONE) {
          return LocalDateTime.parse(value.toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME).atOffset(ZoneOffset.UTC);
        }
      } catch (DateTimeParseException ex) {
        log.debug("Error when coerce date time value", ex);
      }
      return cast(value);
    }
  }

  /**
   * Convert a value from Number JDBCType to Number
   *
   * @see JDBCStatementHelper#LOOKUP_SQL_NUMBER
   */
  protected Object convertNumber(SQLValueProvider valueProvider, JDBCType jdbcType) throws SQLException {
    try {
      return cast(valueProvider.apply(JDBCStatementHelper.LOOKUP_SQL_NUMBER.apply(jdbcType)));
    } catch (SQLException e) {
      log.debug("Error when convert SQL number", e);
      return cast(valueProvider.apply(null));
    }
  }

  /**
   * Convert a value from {@link JDBCType#BINARY}, {@link JDBCType#VARBINARY}, and {@link JDBCType#LONGVARBINARY} datatype to {@link Buffer}.
   * <p>
   * Keep value as it is if the actual value's type is not {@code byte[]}
   */
  protected Object convertBinary(SQLValueProvider valueProvider, JDBCType jdbcType) throws SQLException {
    Object v = getCoerceObject(valueProvider, byte[].class);
    return v instanceof byte[] ? Buffer.buffer((byte[]) v) : cast(v);
  }

  /**
   * Convert a value from {@link JDBCType#STRUCT} datatype to {@link Tuple}
   * <p>
   * Fallback to {@link #convertSpecialType(SQLValueProvider, JDBCType)} if the actual value's type is not {@link Struct}
   */
  protected Object convertStruct(SQLValueProvider valueProvider) throws SQLException {
    Object v = getCoerceObject(valueProvider, Struct.class);
    if (v instanceof Struct) {
      return cast(v);
    }
    return convertSpecialType(valueProvider, JDBCType.STRUCT);
  }

  /**
   * Convert a value from {@link JDBCType#DATALINK} datatype to {@link URL}
   * <p>
   * Keep value as it is if the actual value's type is not {@code URL} or {@code String}
   */
  protected Object convertLink(SQLValueProvider valueProvider) throws SQLException {
    Object v = getCoerceObject(valueProvider, URL.class);
    if (v instanceof URL) {
      return v;
    }
    if (v instanceof String) {
      try {
        return new URL((String) v);
      } catch (MalformedURLException e) {
        throw new SQLException("Unable read data link", e);
      }
    }
    return cast(v);
  }

  /**
   * Convert a value from {@link JDBCType#SQLXML} datatype to {@link Buffer}
   * <p>
   * Fallback to {@link #convertSpecialType(SQLValueProvider, JDBCType)} if the actual value's type is not {@link SQLXML}
   */
  protected Object convertXML(SQLValueProvider valueProvider) throws SQLException {
    Object v = getCoerceObject(valueProvider, SQLXML.class);
    if (v instanceof SQLXML) {
      return streamToBuffer(((SQLXML) v).getBinaryStream(), SQLXML.class);
    }
    return convertSpecialType(valueProvider, JDBCType.SQLXML);
  }

  /**
   * Convert a value from the special jdbc types to string value
   *
   * @return string value
   * @see JDBCType#NULL
   * @see JDBCType#OTHER
   * @see JDBCType#DISTINCT
   * @see JDBCType#REF_CURSOR
   * @see JDBCType#JAVA_OBJECT
   */
  protected Object convertSpecialType(SQLValueProvider valueProvider, JDBCType jdbcType) throws SQLException {
    return Optional.ofNullable(cast(valueProvider.apply(null))).map(Object::toString).orElse(null);
  }

  protected Object getCoerceObject(SQLValueProvider valueProvider, Class<?> cls) throws SQLException {
    try {
      return valueProvider.apply(null);
    } catch (SQLException e) {
      return valueProvider.apply(cls);
    }
  }

  protected Buffer streamToBuffer(InputStream is, Class<?> dataTypeClass) throws SQLException {
    try (InputStream in = is) {
      Buffer buffer = Buffer.buffer(1024);
      byte[] buf = new byte[1024];
      int l;
      while ((l = in.read(buf)) > -1) {
        buffer.appendBytes(buf, 0, l);
      }
      return buffer;
    } catch (IOException ioe) {
      throw new SQLException("Unable to read binary stream from " + dataTypeClass.getName(), ioe);
    }
  }
}
