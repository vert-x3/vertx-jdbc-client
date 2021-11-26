/*
 * Copyright (c) 2011-2021 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.jdbcclient.impl.actions;

import io.vertx.ext.jdbc.impl.actions.JDBCTypeWrapper;
import io.vertx.sqlclient.desc.ColumnDescriptor;

import java.sql.JDBCType;
import java.sql.SQLException;

public class JDBCColumnDescriptor implements ColumnDescriptor {

  private final String columnLabel;
  private final JDBCTypeWrapper jdbcTypeWrapper;

  private JDBCColumnDescriptor(String columnLabel, JDBCTypeWrapper jdbcTypeWrapper) {
    this.columnLabel = columnLabel;
    this.jdbcTypeWrapper = jdbcTypeWrapper;
  }

  @Override
  public String name() {
    return columnLabel;
  }

  @Override
  public boolean isArray() {
    return jdbcType() == JDBCType.ARRAY;
  }

  @Override
  public String typeName() {
    return jdbcTypeWrapper.vendorTypeName();
  }

  /**
   * Use {@link #jdbcTypeWrapper()} when converting an advanced data type depending on the specific database
   *
   * @return the most appropriate {@code JDBCType} or {@code null} if it is advanced type of specific database
   */
  @Override
  public JDBCType jdbcType() {
    return jdbcTypeWrapper.jdbcType();
  }

  /**
   * @return the jdbc type wrapper
   * @see JDBCTypeWrapper
   */
  public JDBCTypeWrapper jdbcTypeWrapper() {
    return this.jdbcTypeWrapper;
  }

  @Override
  public String toString() {
    return "JDBCColumnDescriptor[columnName=(" + columnLabel + "), jdbcTypeWrapper=(" + jdbcTypeWrapper + ")]";
  }

  public static JDBCColumnDescriptor create(JDBCPropertyAccessor<String> columnLabel,
                                            JDBCPropertyAccessor<Integer> vendorTypeNumber,
                                            JDBCPropertyAccessor<String> vendorTypeName,
                                            JDBCPropertyAccessor<String> vendorTypeClassName) throws SQLException {
    return new JDBCColumnDescriptor(columnLabel.get(), JDBCTypeWrapper.of(vendorTypeNumber.get(), vendorTypeName.get(),
      vendorTypeClassName.get()));
  }

  public static JDBCColumnDescriptor wrap(JDBCType jdbcType) {
    return new JDBCColumnDescriptor(null, JDBCTypeWrapper.of(jdbcType));
  }

}
