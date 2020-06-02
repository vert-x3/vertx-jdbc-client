/*
 * Copyright (c) 2018 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vertx.ext.sql.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLRowStream;

import java.util.List;

/**
 * A wrapper that auto closes the connection when underlying `SQLRowStream` closes
 *
 * @author <a href="mailto:ruslan.sennov@gmail.com">Ruslan Sennov</a>
 */
public class RowStreamWrapper implements SQLRowStream {

  private final SQLConnection connection;
  private final SQLRowStream rowStream;

  public RowStreamWrapper(SQLConnection connection, SQLRowStream rowStream) {
    this.connection = connection;
    this.rowStream = rowStream;
  }

  private void closeConnection(Handler<AsyncResult<Void>> handler) {
    connection.close(handler);
  }

  @Override
  public SQLRowStream exceptionHandler(Handler<Throwable> handler) {
    if (handler == null) {
      rowStream.exceptionHandler(null);
    } else {
      rowStream.exceptionHandler(h1 -> closeConnection(h2 -> handler.handle(h1)));
    }
    return this;
  }

  @Override
  public SQLRowStream handler(Handler<JsonArray> handler) {
    rowStream.handler(handler);
    return this;
  }

  @Override
  public SQLRowStream fetch(long amount) {
    rowStream.fetch(amount);
    return this;
  }

  @Override
  public SQLRowStream pause() {
    rowStream.pause();
    return this;
  }

  @Override
  public SQLRowStream resume() {
    rowStream.resume();
    return this;
  }

  @Override
  public SQLRowStream endHandler(Handler<Void> endHandler) {
    if (endHandler == null) {
      rowStream.endHandler(null);
    } else {
      rowStream.endHandler(h1 -> closeConnection(h2 -> endHandler.handle(h1)));
    }
    return this;
  }

  @Override
  public int column(String name) {
    return rowStream.column(name);
  }

  @Override
  public List<String> columns() {
    return rowStream.columns();
  }

  @Override
  public SQLRowStream resultSetClosedHandler(Handler<Void> handler) {
    rowStream.resultSetClosedHandler(handler);
    return this;
  }

  @Override
  public void moreResults() {
    rowStream.moreResults();
  }

  @Override
  public void close() {
    close(null);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    rowStream.close(h1 -> closeConnection(h2 -> {
      if (handler != null) {
        handler.handle(h1);
      }
    }));
  }
}
