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
package io.vertx.ext.jdbc.spi;

import io.vertx.ext.jdbc.impl.actions.SQLValueProvider;
import io.vertx.jdbcclient.impl.actions.JDBCColumnDescriptor;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Represents for JDBC decoder from SQL value to Java value
 * <p>
 * The default decoder provides the best efforts to convert {@code SQL type} to standard {@code Java type} as {@code
 * JDBC 4.2} spec.
 * <p>
 * You can replace it to adapt to a specific SQL driver by creating your owns then includes in the SPI file ({@code
 * META-INF/services/io.vertx.ext.jdbc.spi.JDBCDecoder})
 *
 * @see <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/jdbc_42.html">Mapping of java.sql.Types to
 * SQL types</a>
 * @see io.vertx.ext.jdbc.spi.impl.JDBCDecoderImpl
 * @see java.sql.JDBCType
 * @see java.sql.SQLType
 * @since 4.2.0
 */

public interface JDBCDecoder {

  /**
   * Parse SQL value to Java value
   *
   * @param rs             JDBC result set
   * @param pos            the Database column position
   * @param jdbcTypeLookup JDBCType provider
   * @return java value
   * @throws SQLException if any error in parsing
   * @see ResultSet
   * @see JDBCColumnDescriptorProvider
   * @since 4.2.2
   */
  Object parse(ResultSet rs, int pos, JDBCColumnDescriptorProvider jdbcTypeLookup) throws SQLException;

  /**
   * Parse SQL value to Java value
   *
   * @param cs             JDBC callable statement
   * @param pos            the parameter column position
   * @param jdbcTypeLookup JDBCType provider
   * @return java value
   * @throws SQLException if any error in parsing
   * @see CallableStatement
   * @see JDBCColumnDescriptorProvider
   * @since 4.2.2
   */
  Object parse(CallableStatement cs, int pos, JDBCColumnDescriptorProvider jdbcTypeLookup) throws SQLException;

  /**
   * Convert the SQL value to Java value based on jdbc type
   *
   * @param descriptor    the JDBC column descriptor
   * @param valueProvider the value provider
   * @return java value
   * @see SQLValueProvider
   * @see JDBCColumnDescriptor
   * @since 4.2.2
   */
  Object decode(JDBCColumnDescriptor descriptor, SQLValueProvider valueProvider) throws SQLException;

  /**
   * Try cast SQL value to standard Java value depends on standard JDBC 4.2 type mapping and compatible with Vertx
   * <p>
   * For example: - java.sql.Time -> java.time.LocalTime - java.sql.Timestamp -> java.time.LocalDateTime
   *
   * @param value value
   * @return a presenter value
   * @throws SQLException if any error when casting
   */
  Object cast(Object value) throws SQLException;

}
