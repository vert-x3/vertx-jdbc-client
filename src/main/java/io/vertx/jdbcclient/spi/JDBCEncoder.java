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

import io.vertx.core.json.JsonArray;
import io.vertx.sqlclient.Tuple;

import java.sql.SQLException;

/**
 * Represents for JDBC encoder from Java value to SQL value
 * <p>
 * The default encoder provides the best efforts to convert {@code Java type} to {@code SQL type} as {@code JDBC 4.2} spec.
 * <p>
 * You can replace it to adapt to a specific SQL driver by creating your owns then includes in the SPI file
 * ({@code META-INF/services/io.vertx.ext.jdbc.spi.JDBCEncoder})
 *
 * @see <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/jdbc_42.html">Mapping of java.sql.Types to SQL types</a>
 * @see JDBCEncoderImpl
 * @see java.sql.JDBCType
 * @see java.sql.SQLType
 * @since 4.2.0
 */
public interface JDBCEncoder {

  /**
   * Convert Java input value to SQL value
   *
   * @param input    array input
   * @param pos      column position
   * @param provider JDBCType provider
   * @return SQL value
   * @throws SQLException if any error when convert
   * @see JDBCColumnDescriptorProvider
   */
  Object encode(JsonArray input, int pos, JDBCColumnDescriptorProvider provider) throws SQLException;

  Object encode(Tuple input, int pos, JDBCColumnDescriptorProvider provider) throws SQLException;

}
