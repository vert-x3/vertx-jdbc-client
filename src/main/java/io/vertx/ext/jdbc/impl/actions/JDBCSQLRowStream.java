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
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.SQLRowStream;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
class JDBCSQLRowStream implements SQLRowStream {

  private static final Logger log = LoggerFactory.getLogger(JDBCSQLRowStream.class);
  private static final int ACCUMULATOR_SIZE = 128;

  private final WorkerExecutor exec;
  private final Statement st;
  private final AtomicBoolean paused = new AtomicBoolean(false);
  private final AtomicBoolean ended = new AtomicBoolean(false);
  private final AtomicBoolean stClosed = new AtomicBoolean(false);
  private final AtomicBoolean rsClosed = new AtomicBoolean(false);
  private final AtomicBoolean more = new AtomicBoolean(false);
  private final Deque<JsonArray> accumulator = new ArrayDeque<>(ACCUMULATOR_SIZE);

  private ResultSet rs;
  private int cols;

  private Handler<Throwable> exceptionHandler;
  private Handler<JsonArray> handler;
  private Handler<Void> endHandler;
  private Handler<Void> rsClosedHandler;

  JDBCSQLRowStream(WorkerExecutor exec, Statement st, ResultSet rs) throws SQLException {
    this.exec = exec;
    this.st = st;
    this.rs = rs;

    cols = rs.getMetaData().getColumnCount();
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
  public SQLRowStream exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public SQLRowStream handler(Handler<JsonArray> handler) {
    this.handler = handler;
    // start pumping data once the handler is set
    resume();
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
    while (!paused.get() && !accumulator.isEmpty()) {
      handler.handle(accumulator.pollFirst());
    }
    if (!paused.get()) {
      exec.executeBlocking(this::readRows, res -> {
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
      while (accumulator.size() < ACCUMULATOR_SIZE && rs.next()) {
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
      endHandler.handle(null);
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

      exec.executeBlocking(this::getNextResultSet, res -> {
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
        cols = rs.getMetaData().getColumnCount();
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
      exec.executeBlocking(f -> {
        try {
          closeable.close();
          f.complete();
        } catch (Exception e) {
          f.fail(e);
        }
      }, handler);
    } else {
      if (handler != null) {
        handler.handle(Future.succeededFuture());
      }
    }
  }
}
