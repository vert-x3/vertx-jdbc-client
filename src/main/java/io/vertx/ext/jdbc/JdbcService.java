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

package io.vertx.ext.jdbc;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.ProxyIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.impl.JdbcServiceImpl;
import io.vertx.proxygen.ProxyHelper;

import java.util.List;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
@VertxGen
@ProxyGen
public interface JdbcService {

  static JdbcService create(Vertx vertx, JsonObject config) {
    return new JdbcServiceImpl(vertx, config);
  }

  static JdbcService createEventBusProxy(Vertx vertx, String address) {
    return ProxyHelper.createProxy(JdbcService.class, vertx, address);
  }

  public void execute(String sql, Handler<AsyncResult<Void>> resultHandler) throws RuntimeSqlException;

  public void executeWithTx(String transactionId, String sql, Handler<AsyncResult<Void>> resultHandler) throws RuntimeSqlException;

  public void select(String table, Handler<AsyncResult<List<JsonObject>>> resultHandler);

  public void selectTx(String transactionId, String table, Handler<AsyncResult<JsonObject>> resultHandler);

  /*
  public void selectWithFields(String table, List<String> fields, Handler<AsyncResult<List<JsonObject>>> resultHandler);

  public void selectWithFieldsTx(String transactionId, String table, List<String> fields, Handler<AsyncResult<JsonObject>> resultHandler);
  */

  public void commit(String transactionId);

  public void rollback(String transactionId);

  @ProxyIgnore
  public void start();

  @ProxyIgnore
  public void stop();
}
