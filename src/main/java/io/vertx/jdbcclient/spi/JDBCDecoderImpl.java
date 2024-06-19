/*
 * Copyright (c) 2011-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.jdbcclient.spi;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.jdbcclient.impl.actions.JDBCTypeWrapper;
import io.vertx.jdbcclient.impl.actions.SQLValueProvider;
import io.vertx.jdbcclient.impl.actions.JDBCColumnDescriptor;
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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public class JDBCDecoderImpl implements JDBCDecoder {

  private static final Logger LOG = LoggerFactory.getLogger(JDBCDecoder.class);

  @Override
  public Object parse(ResultSet rs, int pos, JDBCColumnDescriptorProvider jdbcTypeLookup) throws SQLException {
    return decode(jdbcTypeLookup.apply(pos), cls -> cls == null ? rs.getObject(pos) : rs.getObject(pos, cls));
  }

  @Override
  public Object parse(CallableStatement cs, int pos, JDBCColumnDescriptorProvider jdbcTypeLookup) throws SQLException {
    return decode(jdbcTypeLookup.apply(pos), cls -> cls == null ? cs.getObject(pos) : cs.getObject(pos, cls));
  }

  @Override
  public Object decode(JDBCColumnDescriptor descriptor, SQLValueProvider valueProvider) throws SQLException {
    if (descriptor != null) {
      if (descriptor.isArray()) {
        return decodeArray(valueProvider, descriptor);
      }
      if (descriptor.jdbcType() == JDBCType.DATALINK) {
        return decodeLink(valueProvider, descriptor);
      }
      if (descriptor.jdbcType() == JDBCType.SQLXML) {
        return decodeXML(valueProvider, descriptor);
      }
      if (descriptor.jdbcType() == JDBCType.STRUCT) {
        return decodeStruct(valueProvider, descriptor);
      }
      if (descriptor.jdbcTypeWrapper().isBinaryType()) {
        return decodeBinary(valueProvider, descriptor);
      }
      if (descriptor.jdbcTypeWrapper().isNumberType()) {
        return decodeNumber(valueProvider, descriptor);
      }
      if (descriptor.jdbcTypeWrapper().isDateTimeType()) {
        return decodeDateTime(valueProvider, descriptor);
      }
      if (descriptor.jdbcTypeWrapper().isUnhandledType()) {
        return decodeUnhandledType(valueProvider, descriptor);
      }
      if (descriptor.jdbcTypeWrapper().isSpecificVendorType()) {
        return decodeSpecificVendorType(valueProvider, descriptor);
      }
      return cast(getCoerceObject(valueProvider, descriptor.jdbcTypeWrapper().vendorTypeClass()));
    } else {
      return cast(valueProvider.apply(null));
    }
  }

  @Override
  public Object cast(Object value) throws SQLException {
    if (value == null) {
      return null;
    }

    if (value instanceof Array) {
      Array array = (Array) value;
      return this.decodeArray(array, JDBCColumnDescriptor.create(() -> null, array::getBaseType, array::getBaseTypeName,
        () -> null));
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

  protected Object decodeArray(SQLValueProvider valueProvider, JDBCColumnDescriptor descriptor) throws SQLException {
    final Object value = getCoerceObject(valueProvider, descriptor.jdbcTypeWrapper().vendorTypeClass());
    if (value instanceof Array) {
      return decodeArray((Array) value, descriptor);
    }
    return cast(value);
  }

  /**
   * Convert a value from date time JDBCType to Java date time
   *
   * @see JDBCTypeWrapper#isDateTimeType()
   */
  protected Object decodeDateTime(SQLValueProvider valueProvider, JDBCColumnDescriptor descriptor) throws SQLException {
    try {
      return cast(valueProvider.apply(descriptor.jdbcTypeWrapper().vendorTypeClass()));
    } catch (SQLException e) {
      LOG.debug("Error when convert SQL date time. Try coerce value", e);
      Object value = valueProvider.apply(null);
      if (value == null) {
        return null;
      }
      try {
        // Some JDBC drivers (PG driver) treats Timestamp with TimeZone/Time with TimeZone
        // to java.sql.timestamp/java.sql.time/String at system timezone
        // and handles date time data type internally
        // then this code will try parse to OffsetTime/OffsetDateTime at UTC timezone with ISO8601 format
        if (value instanceof Time) {
          return Instant.ofEpochMilli(((Time) value).getTime()).atOffset(ZoneOffset.UTC).toOffsetTime();
        }
        if (descriptor.jdbcType() == JDBCType.TIME) {
          return LocalTime.parse(value.toString()).atOffset(ZoneOffset.UTC);
        }
        if (descriptor.jdbcType() == JDBCType.TIME_WITH_TIMEZONE) {
          return OffsetTime.parse(value.toString()).withOffsetSameInstant(ZoneOffset.UTC);
        }

        if (value instanceof Timestamp) {
          return ((Timestamp) value).toInstant().atOffset(ZoneOffset.UTC);
        }
        if (descriptor.jdbcType() == JDBCType.TIMESTAMP) {
          return LocalDateTime.parse(value.toString()).atOffset(ZoneOffset.UTC);
        }
        if (descriptor.jdbcType() == JDBCType.TIMESTAMP_WITH_TIMEZONE) {
          return OffsetDateTime.parse(value.toString()).withOffsetSameInstant(ZoneOffset.UTC);
        }
      } catch (DateTimeParseException ex) {
        LOG.debug("Error when coerce date time value", ex);
      }
      return cast(value);
    }
  }

  /**
   * Convert a value from Number JDBCType to Number
   *
   * @see JDBCTypeWrapper#isNumberType()
   */
  protected Object decodeNumber(SQLValueProvider valueProvider, JDBCColumnDescriptor descriptor) throws SQLException {
    try {
      return cast(valueProvider.apply(descriptor.jdbcTypeWrapper().vendorTypeClass()));
    } catch (SQLException e) {
      LOG.debug("Error when convert SQL number", e);
      return cast(valueProvider.apply(null));
    }
  }

  /**
   * Convert a value from {@link JDBCTypeWrapper#isBinaryType()} datatype to {@link Buffer}.
   * <p>
   * Keep value as it is if the actual value's type is not {@code byte[]}
   *
   * @see JDBCTypeWrapper#isBinaryType()
   */
  protected Object decodeBinary(SQLValueProvider valueProvider, JDBCColumnDescriptor descriptor) throws SQLException {
    Object v = getCoerceObject(valueProvider, descriptor.jdbcTypeWrapper().vendorTypeClass());
    return v instanceof byte[] ? Buffer.buffer((byte[]) v) : cast(v);
  }

  /**
   * Convert a value from {@link JDBCType#STRUCT} datatype to {@link Tuple}
   * <p>
   * Fallback to {@link #decodeUnhandledType(SQLValueProvider, JDBCColumnDescriptor)} if the actual value's type is not
   * {@link Struct}
   */
  protected Object decodeStruct(SQLValueProvider valueProvider, JDBCColumnDescriptor descriptor) throws SQLException {
    Object v = getCoerceObject(valueProvider, descriptor.jdbcTypeWrapper().vendorTypeClass());
    if (v instanceof Struct) {
      return cast(v);
    }
    return decodeUnhandledType(valueProvider, descriptor);
  }

  /**
   * Convert a value from {@link JDBCType#DATALINK} datatype to {@link URL}
   * <p>
   * Keep value as it is if the actual value's type is not {@code URL} or {@code String}
   */
  protected Object decodeLink(SQLValueProvider valueProvider, JDBCColumnDescriptor descriptor) throws SQLException {
    Object v = getCoerceObject(valueProvider, descriptor.jdbcTypeWrapper().vendorTypeClass());
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
   * Fallback to {@link #decodeUnhandledType(SQLValueProvider, JDBCColumnDescriptor)}} if the actual value's type is not
   * {@link SQLXML}
   */
  protected Object decodeXML(SQLValueProvider valueProvider, JDBCColumnDescriptor descriptor) throws SQLException {
    Object v = getCoerceObject(valueProvider, descriptor.jdbcTypeWrapper().vendorTypeClass());
    if (v instanceof SQLXML) {
      return streamToBuffer(((SQLXML) v).getBinaryStream(), descriptor.jdbcTypeWrapper().vendorTypeClass());
    }
    return decodeUnhandledType(valueProvider, descriptor);
  }

  /**
   * Convert a value from the unhandled data type
   * <p>
   * The default implementation converts any data type to a string value
   *
   * @return value
   * @see JDBCTypeWrapper#isUnhandledType()
   */
  protected Object decodeUnhandledType(SQLValueProvider valueProvider, JDBCColumnDescriptor descriptor)
    throws SQLException {
    LOG.debug("Fallback to string when handling the unhandled JDBCType in Vertx " + descriptor);
    return Optional.ofNullable(cast(valueProvider.apply(null))).map(Object::toString).orElse(null);
  }

  /**
   * Convert a value from the {@code specific SQL vendor data type} to {@code Java} value
   * <p>
   * The default implementation converts any data type to a string value
   *
   * @return value
   * @see JDBCTypeWrapper#isSpecificVendorType()
   */
  protected Object decodeSpecificVendorType(SQLValueProvider valueProvider, JDBCColumnDescriptor descriptor)
    throws SQLException {
    LOG.debug("Fallback to string when handling the specific SQL vendor data type " + descriptor);
    return Optional.ofNullable(cast(valueProvider.apply(null))).map(Object::toString).orElse(null);
  }

  protected Object decodeArray(Array value, JDBCColumnDescriptor baseType) throws SQLException {
    try {
      Object arr = value.getArray();
      if (arr != null) {
        int len = java.lang.reflect.Array.getLength(arr);
        Object[] castedArray = new Object[len];
        for (int i = 0; i < len; i++) {
          int index = i;
          castedArray[i] = decode(baseType, cls -> java.lang.reflect.Array.get(arr, index));
        }
        return castedArray;
      }
      return value;
    } finally {
      value.free();
    }
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
