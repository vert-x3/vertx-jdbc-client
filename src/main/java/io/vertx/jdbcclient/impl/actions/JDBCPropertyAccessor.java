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

package io.vertx.jdbcclient.impl.actions;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

public interface JDBCPropertyAccessor<T> {

  Logger LOG = LoggerFactory.getLogger(JDBCColumnDescriptor.class);

  T get() throws SQLException;

  static <T> JDBCPropertyAccessor<T> create(JDBCPropertyAccessor<T> accessor) {
    return create(accessor, null);
  }

  static <T> JDBCPropertyAccessor<T> create(JDBCPropertyAccessor<T> accessor, T fallbackIfUnsupported) {
    return () -> {
      try {
        return accessor.get();
      } catch (SQLFeatureNotSupportedException e) {
        LOG.debug("Unsupported access properties in SQL metadata", e);
        return fallbackIfUnsupported;
      }
    };
  }

  static JDBCPropertyAccessor<Integer> jdbcType(JDBCPropertyAccessor<Integer> accessor) {
    return create(accessor, JDBCType.OTHER.getVendorTypeNumber());
  }

}
