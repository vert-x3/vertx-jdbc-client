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
import io.vertx.core.queue.Queue;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.sql.SQLRowStream;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
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
  private final Queue<JsonArray> pending;
  private final AtomicBoolean ended = new AtomicBoolean(false);
  private final AtomicBoolean stClosed = new AtomicBoolean(false);
  private final AtomicBoolean rsClosed = new AtomicBoolean(false);
  private final AtomicBoolean more = new AtomicBoolean(false);

  private ResultSet rs;
  private ResultSetMetaData metaData;
  private List<String> columns;
  private int cols;

  private Handler<Throwable> exceptionHandler;
  private Handler<Void> endHandler;
  private Handler<Void> rsClosedHandler;

  JDBCSQLRowStream(ContextInternal ctx, TaskQueue statementsQueue, Statement st, ResultSet rs, int fetchSize) throws SQLException {
    this.ctx = ctx;
    this.st = st;
    this.fetchSize = fetchSize;
    this.rs = rs;
    this.statementsQueue = statementsQueue;
    this.pending = Queue.<JsonArray>queue(ctx, fetchSize)
      .writableHandler(v -> readBatch())
      .emptyHandler(v -> checkEndHandler());

    metaData = rs.getMetaData();
    cols = metaData.getColumnCount();
    stClosed.set(false);
    rsClosed.set(false);
    // the first rs is populated in the constructor
    more.set(true);
  }

  private void checkEndHandler() {
    if (ended.get() && pending.isEmpty()) {
      if (endHandler != null) {
        endHandler.handle(null);
      }
    }
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
    pending.handler(handler);
    if (handler != null && pending.isWritable()) {
      readBatch();
    }
    return this;
  }

  @Override
  public SQLRowStream pause() {
    pending.pause();
    return this;
  }

  @Override
  public ReadStream<JsonArray> fetch(long amount) {
    pending.take(amount);
    return this;
  }

  @Override
  public SQLRowStream resume() {
    pending.resume();
    return this;
  }

  private void readBatch() {
    if (!rsClosed.get()) {
      ctx.<List<JsonArray>>executeBlocking(fut -> {
        try {
          List<JsonArray> rows = new ArrayList<>(fetchSize);
          for (int i = 0;i < fetchSize && rs.next();i++) {
            JsonArray result = new JsonArray();
            for (int j = 1; j <= cols; j++) {
              Object res = JDBCStatementHelper.convertSqlValue(rs.getObject(j));
              if (res != null) {
                result.add(res);
              } else {
                result.addNull();
              }
            }
            rows.add(result);
          }
          fut.complete(rows);
        } catch (SQLException e) {
          fut.fail(e);
        }
      }, statementsQueue, ar -> {
        if (ar.succeeded()) {
          List<JsonArray> rows = ar.result();
          if (rows.isEmpty()) {
            empty(null);
          } else if (pending.addAll(rows)) {
            readBatch();
          }
        } else {
          empty(ar.cause());
        }
      });
    }
  }

  private void empty(Throwable err) {
    // mark as ended if the handler was registered too late
    ended.set(true);
    // automatically close resources

    if (rsClosedHandler != null) {
      // only close the result set and notify
      close0(c -> {
        if (err != null) {
          if (exceptionHandler != null) {
            exceptionHandler.handle(err);
          } else {
            log.debug(err);
          }
        } else {
          rsClosedHandler.handle(null);
        }
      });
    } else {
      // default behavior close result set + statement
      close(c -> {
        if (err != null) {
          if (exceptionHandler != null) {
            exceptionHandler.handle(err);
          } else {
            log.debug(err);
          }
        } else {
          checkEndHandler();
        }
      });
    }
  }

  @Override
  public SQLRowStream endHandler(Handler<Void> handler) {
    this.endHandler = handler;
    checkEndHandler();
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
        // paused.set(true);
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
