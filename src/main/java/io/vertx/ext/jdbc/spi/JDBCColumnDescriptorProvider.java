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

import io.vertx.jdbcclient.impl.actions.JDBCColumnDescriptor;
import io.vertx.jdbcclient.impl.actions.JDBCPropertyAccessor;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * A shortcut provider that get a column information in the runtime SQL result or parameter metadata
 *
 * @since 4.2.2
 */
@FunctionalInterface
public interface JDBCColumnDescriptorProvider {

  /**
   * @deprecated Implementations should prefer {@link #fromParameterMetaData(ParameterMetaData)}
   * Create provider by parameter statement
   *
   * @param statement the prepared statement
   * @return a new {@code JDBCTypeProvider} instance
   * @see java.sql.PreparedStatement
   */
  @Deprecated
  static JDBCColumnDescriptorProvider fromParameter(PreparedStatement statement) {
    return col -> JDBCColumnDescriptor.create(() -> null,
      JDBCPropertyAccessor.jdbcType(() -> statement.getParameterMetaData().getParameterType(col)),
      JDBCPropertyAccessor.create(() -> statement.getParameterMetaData().getParameterTypeName(col)),
      JDBCPropertyAccessor.create(() -> statement.getParameterMetaData().getParameterClassName(col)));
  }

  /**
   * Create provider by the parameter metadata
   *
   * @param metaData the parameter metadata
   * @return a new {@code JDBCTypeProvider} instance
   * @see java.sql.ResultSetMetaData
   */
  static JDBCColumnDescriptorProvider fromParameterMetaData(ParameterMetaData metaData) {
    return col -> JDBCColumnDescriptor.create(() -> null,
      JDBCPropertyAccessor.jdbcType(() -> metaData.getParameterType(col)),
      JDBCPropertyAccessor.create(() -> metaData.getParameterTypeName(col)),
      JDBCPropertyAccessor.create(() -> metaData.getParameterClassName(col)));
  }

  /**
   * @deprecated Implementations should prefer {@link #fromResultMetaData(ResultSetMetaData)}
   * Create provider by result set metadata
   *
   * @param rs the result set
   * @return a new {@code JDBCTypeProvider} instance
   * @see java.sql.ResultSetMetaData
   */
  @Deprecated
  static JDBCColumnDescriptorProvider fromResult(ResultSet rs) {
    return col -> JDBCColumnDescriptor.create(JDBCPropertyAccessor.create(() -> rs.getMetaData().getColumnLabel(col)),
      JDBCPropertyAccessor.jdbcType(() -> rs.getMetaData().getColumnType(col)),
      JDBCPropertyAccessor.create(() -> rs.getMetaData().getColumnTypeName(col)),
      JDBCPropertyAccessor.create(() -> rs.getMetaData().getColumnClassName(col)));
  }

  /**
   * Create provider by result set metadata
   *
   * @param metaData the result set
   * @return a new {@code JDBCTypeProvider} instance
   * @see java.sql.ResultSetMetaData
   */
  static JDBCColumnDescriptorProvider fromResultMetaData(ResultSetMetaData metaData) {
    return col -> JDBCColumnDescriptor.create(JDBCPropertyAccessor.create(() -> metaData.getColumnLabel(col)),
      JDBCPropertyAccessor.jdbcType(() -> metaData.getColumnType(col)),
      JDBCPropertyAccessor.create(() -> metaData.getColumnTypeName(col)),
      JDBCPropertyAccessor.create(() -> metaData.getColumnClassName(col)));
  }

  /**
   * Get a column descriptor
   *
   * @param column column index
   * @return the column descriptor
   * @throws SQLException sql exception
   * @see JDBCColumnDescriptor
   */
  JDBCColumnDescriptor apply(int column) throws SQLException;

}
