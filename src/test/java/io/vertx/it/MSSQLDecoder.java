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

package io.vertx.it;

import io.vertx.ext.jdbc.impl.actions.SQLValueProvider;
import io.vertx.ext.jdbc.spi.impl.JDBCDecoderImpl;
import io.vertx.jdbcclient.impl.actions.JDBCColumnDescriptor;
import microsoft.sql.DateTimeOffset;
import microsoft.sql.Types;

import java.sql.SQLException;

public class MSSQLDecoder extends JDBCDecoderImpl {

  @Override
  protected Object decodeSpecificVendorType(SQLValueProvider valueProvider, JDBCColumnDescriptor descriptor) throws SQLException {
    final Object value = valueProvider.apply(null);
    if (value == null) {
      return null;
    }
    if (!descriptor.jdbcTypeWrapper().isSpecificVendorType()) {
      return value.toString();
    }
    if (descriptor.jdbcTypeWrapper().vendorTypeNumber() == Types.DATETIMEOFFSET) {
      if (value instanceof DateTimeOffset) {
        return ((DateTimeOffset) value).getOffsetDateTime();
      }
    }
    return value.toString();
  }

}
