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

import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.internal.QueryResultHandler;
import io.vertx.sqlclient.internal.RowDesc;

import java.util.ArrayList;
import java.util.List;

public class JDBCResponse<R> {

  static class RS<R> {
    R holder;
    int size;
    RowDesc desc;

    RS(R holder, RowDesc desc, int size) {
      this.holder = holder;
      this.desc = desc;
      this.size = size;
    }
  }

  private final int update;
  private List<RS<R>> rs;
  private Row ids;
  private List<RS<R>> output;
  private R empty;

  public JDBCResponse(int updateCount) {
    this.update = updateCount;
  }

  public void push(R decodeResultSet, RowDesc desc, int size) {
    if (rs == null) {
      rs = new ArrayList<>();
    }
    rs.add(new RS<>(decodeResultSet, desc, size));
  }

  public void returnedKeys(Row keys) {
    this.ids = keys;
  }

  public void empty(R apply) {
    this.empty = apply;
  }

  public void outputs(R decodeResultSet, RowDesc desc, int size) {
    if (output == null) {
      output = new ArrayList<>();
    }
    output.add(new RS<>(decodeResultSet, desc, size));
  }

  public void handle(QueryResultHandler<R> handler) {
    if (rs != null) {
      for (RS<R> rs : this.rs) {
        handler.handleResult(update, rs.size, rs.desc, rs.holder, null);
        if (ids != null) {
          handler.addProperty(JDBCPool.GENERATED_KEYS, ids);
        }
      }
    }
    if (output != null) {
      for (RS<R> rs : this.output) {
        handler.handleResult(update, rs.size, null, rs.holder, null);
        handler.addProperty(JDBCPool.OUTPUT, true);
      }
    }
    if (rs == null && output == null) {
      handler.handleResult(update, -1, null, empty, null);
      if (ids != null) {
        handler.addProperty(JDBCPool.GENERATED_KEYS, ids);
      }
    }
  }
}
