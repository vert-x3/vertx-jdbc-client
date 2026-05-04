/*
 * Copyright (c) 2011-2026 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.jdbcclient.impl;

import io.vertx.jdbcclient.RowsList;
import io.vertx.sqlclient.Row;

import java.util.List;

public class RowsListImpl implements RowsList {
  public final List<Row> rows;

  public RowsListImpl(List<Row> rows) {
    this.rows = rows;
  }

  @Override
  public List<Row> rows() {
    return rows;
  }
}
