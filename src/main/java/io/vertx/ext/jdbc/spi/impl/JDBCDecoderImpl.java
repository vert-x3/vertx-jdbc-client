package io.vertx.ext.jdbc.spi.impl;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.ext.jdbc.spi.JDBCDecoder;
import io.vertx.sqlclient.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
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
        return cast(valueProvider.apply(Array.class));
      case BLOB:
        return cast(valueProvider.apply(Blob.class));
      case CLOB:
      case NCLOB:
        return cast(valueProvider.apply(Clob.class));
      case BIT:
      case BOOLEAN:
        return cast(valueProvider.apply(Boolean.class));
      case CHAR:
      case VARCHAR:
      case LONGVARCHAR:
      case NCHAR:
      case NVARCHAR:
      case LONGNVARCHAR:
        return cast(valueProvider.apply(String.class));
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case REAL:
      case DOUBLE:
      case NUMERIC:
      case DECIMAL:
        return convertNumber(jdbcType, valueProvider);
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIME_WITH_TIMEZONE:
      case TIMESTAMP_WITH_TIMEZONE:
        return convertDateTime(jdbcType, valueProvider);
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
        return cast(valueProvider.apply(null));
      case ROWID:
        return cast(valueProvider.apply(RowId.class));
      case STRUCT:
        return cast(valueProvider.apply(Struct.class));
      case NULL:
      case OTHER:
      case JAVA_OBJECT:
      case DISTINCT:
      case REF:
      case DATALINK:
      case SQLXML:
      case REF_CURSOR:
        log.debug("Fallback to string when handle JDBCType " + jdbcType);
        break;
    }
    return Optional.ofNullable(cast(valueProvider.apply(null))).map(Object::toString).orElse(null);
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
      return convertBlob((Blob) value);
    }

    if (value instanceof Clob) {
      return convertClob((Clob) value);
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

  protected Object convertDateTime(JDBCType jdbcType, SQLValueProvider valueProvider) throws SQLException {
    try {
      return cast(valueProvider.apply(LOOKUP_SQL_DATETIME.apply(jdbcType)));
    } catch (SQLException e) {
      log.debug("Error when convert SQL date time. Try coerce value", e);
      Object value = valueProvider.apply(null);
      if (value == null) {
        return null;
      }
      try {
        // Some JDBC drivers (PG driver) treats Timestamp with TimeZone/Time with TimeZone
        // to java.sql.timestamp/java.sql.time/String
        // and handle date time data type internally
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

  protected Object convertNumber(JDBCType jdbcType, SQLValueProvider valueProvider) throws SQLException {
    try {
      return cast(valueProvider.apply(LOOKUP_SQL_NUMBER.apply(jdbcType)));
    } catch (SQLException e) {
      log.debug("Error when convert SQL number", e);
      return cast(valueProvider.apply(null));
    }
  }

  protected String convertClob(Clob data) throws SQLException {
    if (data == null) {
      return null;
    }

    if (data.length() == 0L) {
      return "";
    }

    StringBuilder buffer = new StringBuilder();
    char[] buf = new char[1024];
    try (Reader in = data.getCharacterStream()) {
      int l;
      while ((l = in.read(buf)) > -1) {
        buffer.append(buf, 0, l);
      }
    } catch (IOException ioe) {
      throw new SQLException("Unable to read character stream from Clob.", ioe);
    }
    return buffer.toString();
  }

  protected Buffer convertBlob(Blob data) throws SQLException {
    if (data == null) {
      return null;
    }

    if (data.length() == 0L) {
      return Buffer.buffer(0);
    }

    Buffer buffer = Buffer.buffer(1024);
    byte[] buf = new byte[1024];
    try (InputStream in = data.getBinaryStream()) {
      int l;
      while ((l = in.read(buf)) > -1) {
        buffer.appendBytes(buf, 0, l);
      }
    } catch (IOException ioe) {
      throw new SQLException("Unable to read binary stream from Blob.", ioe);
    }
    return buffer;
  }

}
