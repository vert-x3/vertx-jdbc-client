/*
 * Copyright (c) 2011-2018 The original author or authors
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

package io.vertx.ext.sql;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;

import java.util.List;

/**
 * Collection of static methods that deal with {@link Handler}s within the Context of a SQLConnection or related SQL-Objects
 *
 * @author <a href="mailto:jvelez@chibchasoft.com">Juan VELEZ</a>
 */
final class HandlerUtil {
  /**
   * Returns a {@link Handler} for {@link AsyncResult} of {@link ResultSet} that will delegate the outcome of the result
   * (only a single row) to the passed handler. If the {@code AsyncResult} is failed then the failure will be propagated
   * to the delegate. If the {@code ResultSet} is null or empty, a null will be the result passed to the delegate handler
   * @param handler the target handler of the result
   * @return the new handler
   */
  static Handler<AsyncResult<ResultSet>> handleResultSetSingleRow(Handler<AsyncResult<@Nullable JsonArray>> handler) {
    return ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        final ResultSet rs = ar.result();
        if (rs == null) {
          handler.handle(Future.succeededFuture());
        } else {
          List<JsonArray> results = rs.getResults();
          if (results == null) {
            handler.handle(Future.succeededFuture());
          } else {
            if (results.size() > 0) {
              handler.handle(Future.succeededFuture(results.get(0)));
            } else {
              handler.handle(Future.succeededFuture());
            }
          }
        }
      }
    };
  }

  /**
   * Returns a {@link Handler} for {@link AsyncResult} of a {@code SQL} result that regardless of the outcome will close
   * the passed {@link SQLConnection}. If the {@code AsyncResult} is failed then the failure will be propagated
   * to the delegate handler. If it was successful, the result will be propagated to the delegate handler. In any case
   * the delegation to the handler will be done as part of closing the {@code connection}.
   * @param conn the connection to close
   * @param handler the target handler of the result
   * @return the new handler
   */
  static <T> Handler<AsyncResult<T>> closeAndHandleResult(SQLConnection conn, Handler<AsyncResult<T>> handler) {
    return ar -> {
      if (ar.failed()) {
        conn.close(close -> {
          if (close.failed()) {
            handler.handle(Future.failedFuture(close.cause()));
          } else {
            handler.handle(Future.failedFuture(ar.cause()));
          }
        });
      } else {
        conn.close(close -> {
          if (close.failed()) {
            handler.handle(Future.failedFuture(close.cause()));
          } else {
            handler.handle(Future.succeededFuture(ar.result()));
          }
        });
      }
    };
  }
}
