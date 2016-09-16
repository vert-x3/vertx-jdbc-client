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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
class JDBCSQLRowStream implements SQLRowStream {

    private static final Logger log = LoggerFactory.getLogger(JDBCSQLRowStream.class);

    private final WorkerExecutor exec;
    private final Statement st;
    private final ResultSet rs;

    private int cols;
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicBoolean stClosed = new AtomicBoolean(false);
    private final AtomicBoolean rsClosed = new AtomicBoolean(false);

    private Handler<Throwable> exceptionHandler = log::error;
    private Handler<JsonArray> handler;
    private Handler<Void> endHandler;

    JDBCSQLRowStream(WorkerExecutor exec, Statement st, ResultSet rs) throws SQLException {
        this.exec = exec;
        this.st = st;
        this.rs = rs;
        cols = rs.getMetaData().getColumnCount();
        paused.set(false);
        stClosed.set(false);
        rsClosed.set(false);
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
        paused.compareAndSet(true, false);
        nextRow();
        return this;
    }

    private void nextRow() {
        if (!paused.get()) {
            exec.executeBlocking(this::readRow, res -> {
                if (res.failed()) {
                    exceptionHandler.handle(res.cause());
                } else {
                    final JsonArray row = res.result();
                    // no more data
                    if (row == null) {
                        endHandler.handle(null);
                    } else {
                        handler.handle(row);
                        nextRow();
                    }
                }
            });
        }
    }

    private void readRow(Future<JsonArray> fut) {
        try {
            if (rs.next()) {
                JsonArray result = new JsonArray();
                for (int i = 1; i <= cols; i++) {
                    Object res = JDBCStatementHelper.convertSqlValue(rs.getObject(i));
                    if (res != null) {
                        result.add(res);
                    } else {
                        result.addNull();
                    }
                }
                fut.complete(result);
            } else {
                fut.complete();
            }
        } catch (SQLException e) {
            fut.fail(e);
        }
    }

    @Override
    public SQLRowStream endHandler(Handler<Void> handler) {
        this.endHandler = v -> close(res -> {
            if (res.failed()) {
                exceptionHandler.handle(res.cause());
            } else {
                handler.handle(null);
            }
        });
        return this;
    }

    @Override
    public void close(Handler<AsyncResult<Void>> handler) {
        // make sure we stop pumping data
        pause();

        // close the cursor
        close(rs, rsClosed, res -> {
            // close the statement
            close(st, stClosed, handler);
        });
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
            handler.handle(Future.succeededFuture());
        }
    }
}
