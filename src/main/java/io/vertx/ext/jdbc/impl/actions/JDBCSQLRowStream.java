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
package io.vertx.ext.jdbc.impl.actions;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.SQLRowStream;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
class JDBCSQLRowStream implements SQLRowStream {

  private static final Logger log = LoggerFactory.getLogger(JDBCSQLRowStream.class);

  private final ContextInternal ctx;
  private final TaskQueue statementsQueue;
  private final Statement st;
  private final int fetchSize;
  private final AtomicBoolean paused = new AtomicBoolean(false);
  private final AtomicBoolean ended = new AtomicBoolean(false);
  private final AtomicBoolean stClosed = new AtomicBoolean(false);
  private final AtomicBoolean rsClosed = new AtomicBoolean(false);
  private final AtomicBoolean more = new AtomicBoolean(false);
  private final Deque<JsonArray> accumulator;

  private ResultSet rs;
  private ResultSetMetaData metaData;
  private List<String> columns;
  private int cols;

  private Handler<Throwable> exceptionHandler;
  private Handler<JsonArray> handler;
  private Handler<Void> endHandler;
  private Handler<Void> rsClosedHandler;

  JDBCSQLRowStream(ContextInternal ctx, TaskQueue statementsQueue, Statement st, ResultSet rs, int fetchSize) throws SQLException {
    this.ctx = ctx;
    this.st = st;
    this.fetchSize = fetchSize;
    this.rs = rs;
    this.statementsQueue = statementsQueue;

    accumulator = new ArrayDeque<>(fetchSize);
    metaData = rs.getMetaData();
    cols = metaData.getColumnCount();
    paused.set(true);
    stClosed.set(false);
    rsClosed.set(false);
    // the first rs is populated in the constructor
    more.set(true);
  }

  @Override
  public int column(String name) {
    try {
      return rs.findColumn(name) - 1;
    } catch (SQLException e) {
      return -1;
    }
  }

  @Override
  public List<String> columns() {
    if (columns == null) {
      try {
        if (cols > 0) {
          final List<String> columns = new ArrayList<>(cols);
          for (int i = 0; i < cols; i++) {
            columns.add(i, metaData.getColumnName(i + 1));
          }
          this.columns = Collections.unmodifiableList(columns);
        } else {
          this.columns = Collections.emptyList();
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    return columns;
  }

  @Override
  public SQLRowStream exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public SQLRowStream handler(Handler<JsonArray> handler) {
    if ((this.handler = handler) != null) {
      // start pumping data once the handler is set
      resume();
    }
    return this;
  }

  @Override
  public SQLRowStream pause() {
    paused.compareAndSet(false, true);
    return this;
  }

  @Override
  public SQLRowStream resume() {
    if (paused.compareAndSet(true, false)) {
      nextRow();
    }
    return this;
  }

  private void nextRow() {
    // here paused.get() act as volatile read / memory barrier and it must be done before the accumulator read
    // in order to create an happens-before relationship
    if (!paused.get()) {
      // here paused.get() guarantees us that stream is open
      // accumulator should be read after the volatile, so this condition cannot be reordered
      while (!paused.get() && !accumulator.isEmpty()) {
        handler.handle(accumulator.pollFirst());
      }
    }
    if (!paused.get()) {
      ctx.executeBlocking(this::readRows, statementsQueue, res -> {
        if (res.failed()) {
          if (exceptionHandler != null) {
            exceptionHandler.handle(res.cause());
          } else {
            log.debug(res.cause());
          }
        } else {
          // no more data
          if (accumulator.isEmpty()) {
            // mark as ended if the handler was registered too late
            ended.set(true);
            // automatically close resources

            if (rsClosedHandler != null) {
              // only close the result set and notify
              close0(c -> {
                if (res.failed()) {
                  if (exceptionHandler != null) {
                    exceptionHandler.handle(res.cause());
                  } else {
                    log.debug(res.cause());
                  }
                } else {
                  rsClosedHandler.handle(null);
                }
              });
            } else {
              // default behavior close result set + statement
              close(c -> {
                if (res.failed()) {
                  if (exceptionHandler != null) {
                    exceptionHandler.handle(res.cause());
                  } else {
                    log.debug(res.cause());
                  }
                } else {
                  if (endHandler != null) {
                    endHandler.handle(null);
                  }
                }
              });
            }
          } else {
            nextRow();
          }
        }
      });
    }
  }

  private void readRows(Future<Void> fut) {
    try {
      while (accumulator.size() < fetchSize && rs.next()) {
        JsonArray result = new JsonArray();
        for (int i = 1; i <= cols; i++) {
          Object res = JDBCStatementHelper.convertSqlValue(rs.getObject(i));
          if (res != null) {
            result.add(res);
          } else {
            result.addNull();
          }
        }
        accumulator.add(result);
      }
      // paused.set() act as volatile store / memory barrier and it must be done after the accumulator write
      // in order to create an happens-before relationship
      paused.compareAndSet(false, false);
      fut.complete();
    } catch (SQLException e) {
      fut.fail(e);
    }
  }

  @Override
  public SQLRowStream endHandler(Handler<Void> handler) {
    this.endHandler = handler;
    // registration was late but we're already ended, notify
    if (ended.compareAndSet(true, false)) {
      // only notify once
      if (endHandler != null) {
        endHandler.handle(null);
      }
    }
    return this;
  }

  private void close0(Handler<AsyncResult<Void>> handler) {
    // make sure we stop pumping data
    pause();
    // close the cursor
    close(rs, rsClosed, handler);
  }

  @Override
  public void close() {
    close(null);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    close0(res -> {
      // close the statement
      close(st, stClosed, handler);
    });
  }

  @Override
  public SQLRowStream resultSetClosedHandler(Handler<Void> handler) {
    this.rsClosedHandler = handler;
    return this;
  }

  @Override
  public void moreResults() {
    if (more.compareAndSet(true, false)) {
      // pause streaming if rs is not complete
      pause();

      ctx.executeBlocking(this::getNextResultSet, statementsQueue, res -> {
        if (res.failed()) {
          if (exceptionHandler != null) {
            exceptionHandler.handle(res.cause());
          } else {
            log.debug(res.cause());
          }
        } else {
          if (more.get()) {
            resume();
          } else {
            if (endHandler != null) {
              endHandler.handle(null);
            }
          }
        }
      });
    }
  }

  private void getNextResultSet(Future<Void> f) {
    try {
      // close if not already closed
      if (rsClosed.compareAndSet(false, true)) {
        rs.close();
      }
      // is there more rs data?
      if (st.getMoreResults()) {
        rs = st.getResultSet();
        metaData = rs.getMetaData();
        cols = metaData.getColumnCount();
        columns = null;
        // reset
        paused.set(true);
        stClosed.set(false);
        rsClosed.set(false);
        more.set(true);
      }

      f.complete();
    } catch (SQLException e) {
      f.fail(e);
    }
  }

  private void close(AutoCloseable closeable, AtomicBoolean lock, Handler<AsyncResult<Void>> handler) {
    if (lock.compareAndSet(false, true)) {
      ctx.executeBlocking(f -> {
        try {
          closeable.close();
          f.complete();
        } catch (Exception e) {
          f.fail(e);
        }
      }, statementsQueue, handler);
    } else {
      if (handler != null) {
        handler.handle(Future.succeededFuture());
      }
    }
  }
}
