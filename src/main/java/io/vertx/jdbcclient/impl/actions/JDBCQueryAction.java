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

import io.vertx.jdbcclient.spi.JDBCColumnDescriptorProvider;
import io.vertx.jdbcclient.spi.JDBCDecoder;
import io.vertx.jdbcclient.SqlOptions;
import io.vertx.jdbcclient.impl.JDBCRow;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.RowDesc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public abstract class JDBCQueryAction<C, R> extends AbstractJDBCAction<JDBCResponse<R>> {

  private final Collector<Row, C, R> collector;

  public JDBCQueryAction(JDBCStatementHelper helper, SqlOptions options, Collector<Row, C, R> collector) {
    super(helper, options);
    this.collector = collector;
  }

  protected JDBCResponse<R> decode(Statement statement, boolean returnedResultSet, boolean returnedKeys,
                                   CallableOutParams outParams) throws SQLException {

    final JDBCResponse<R> response = new JDBCResponse<>(statement.getUpdateCount());

    if (returnedResultSet) {
      // normal return only
      while (returnedResultSet) {
        try (ResultSet rs = statement.getResultSet()) {
          decodeResultSet(rs, response);
          if (returnedKeys) {
            decodeReturnedKeys(statement, response);
          }
        }
        returnedResultSet = statement.getMoreResults();
      }
    } else {
      collector.accumulator();
      // first rowset includes the output results
      C container = collector.supplier().get();

      response.empty(collector.finisher().apply(container));
      if (returnedKeys) {
        decodeReturnedKeys(statement, response);
      }
    }

    if (!outParams.isEmpty()) {
      decodeOutput((CallableStatement) statement, outParams, response);
    }

    return response;
  }

  protected JDBCResponse<R> decode(Statement statement, int[] returnedBatchResult, boolean returnedKeys) throws SQLException {
    final JDBCResponse<R> response = new JDBCResponse<>(returnedBatchResult.length);

    BiConsumer<C, Row> accumulator = collector.accumulator();

    RowDesc desc = new JDBCRowDesc(new ColumnDescriptor[0]);
    C container = collector.supplier().get();
    for (int result : returnedBatchResult) {
      Row row = new JDBCRow(desc);
      row.addValue(result);
      accumulator.accept(container, row);
    }

    response.push(collector.finisher().apply(container), desc, returnedBatchResult.length);

    if (returnedBatchResult.length != 0) {
      // no queries were executed
      if (returnedKeys) {
        decodeReturnedKeys(statement, response);
      }
    }

    return response;
  }

  private void decodeResultSet(ResultSet rs, JDBCResponse<R> response) throws SQLException {
    BiConsumer<C, Row> accumulator = collector.accumulator();

    ResultSetMetaData metaData = rs.getMetaData();
    JDBCColumnDescriptorProvider provider = JDBCColumnDescriptorProvider.fromResultMetaData(metaData);
    RowDesc desc = new JDBCRowDesc(provider, metaData.getColumnCount());

    C container = collector.supplier().get();
    int size = 0;
    while (rs.next()) {
      size++;
      Row row = new JDBCRow(desc);
      for (int i = 1; i <= desc.columnDescriptor().size(); i++) {
        row.addValue(helper.getDecoder().parse(rs, i, provider));
      }
      accumulator.accept(container, row);
    }

    response.push(collector.finisher().apply(container), desc, size);
  }

  private R decodeRawResultSet(ResultSet rs) throws SQLException {
    BiConsumer<C, Row> accumulator = collector.accumulator();

    // List<String> columnNames = new ArrayList<>();
    ResultSetMetaData metaData = rs.getMetaData();
    JDBCColumnDescriptorProvider provider = JDBCColumnDescriptorProvider.fromResultMetaData(metaData);
    int cols = metaData.getColumnCount();
    RowDesc desc = new JDBCRowDesc(provider, cols);
    C container = collector.supplier().get();

    while (rs.next()) {
      Row row = new JDBCRow(desc);
      for (int i = 1; i <= cols; i++) {
        row.addValue(helper.getDecoder().parse(rs, i, provider));
      }
      accumulator.accept(container, row);
    }

    return collector.finisher().apply(container);
  }

  private void decodeOutput(CallableStatement cs, CallableOutParams outParams, JDBCResponse<R> output) throws SQLException {
    BiConsumer<C, Row> accumulator = collector.accumulator();

    // first rowset includes the output results
    C container = collector.supplier().get();
    // the result is unlabeled
    ParameterMetaData md = new CachedParameterMetaData(cs).putOutParams(outParams);
    JDBCColumnDescriptorProvider provider = JDBCColumnDescriptorProvider.fromParameterMetaData(md);
    RowDesc desc = new JDBCRowDesc(provider, outParams.size());
    Row row = new JDBCRow(desc);
    JDBCDecoder decoder = helper.getDecoder();
    for (Integer idx : outParams.keySet()) {
      // SQL client is 0 index based
      final Object o = cs.getObject(idx);
      if (o instanceof ResultSet) {
        row.addValue(decodeRawResultSet((ResultSet) o));
      } else {
        row.addValue(decoder.parse(cs, idx, provider));
      }
    }

    accumulator.accept(container, row);

    R result = collector.finisher().apply(container);

    output.outputs(result, desc, 1);
  }

  private void decodeReturnedKeys(Statement statement, JDBCResponse<R> response) throws SQLException {
    Row keys = null;

    ResultSet keysRS = statement.getGeneratedKeys();

    if (keysRS != null) {
      if (keysRS.next()) {
        // only try to access metadata if there are rows
        ResultSetMetaData metaData = keysRS.getMetaData();
        if (metaData != null) {
          JDBCColumnDescriptorProvider provider = JDBCColumnDescriptorProvider.fromResultMetaData(metaData);
          int cols = metaData.getColumnCount();
          if (cols > 0) {
            RowDesc keysDesc = new JDBCRowDesc(provider, cols);

            keys = new JDBCRow(keysDesc);
            for (int i = 1; i <= cols; i++) {
              keys.addValue(helper.getDecoder().parse(keysRS, i, provider));
            }
          }
          response.returnedKeys(keys);
        }
      }
    }
  }

  boolean returnAutoGeneratedKeys(Connection conn) {
    boolean autoGeneratedKeys = options == null || options.isAutoGeneratedKeys();
    boolean autoGeneratedIndexes = options != null && options.getAutoGeneratedKeysIndexes() != null && options.getAutoGeneratedKeysIndexes().size() > 0;
    // even though the user wants it, the DBMS may not support it
    if (autoGeneratedKeys || autoGeneratedIndexes) {
      try {
        DatabaseMetaData dbmd = conn.getMetaData();
        if (dbmd != null) {
          return dbmd.supportsGetGeneratedKeys();
        }
      } catch (SQLException e) {
        // ignore...
      }
    }
    return false;
  }

}
