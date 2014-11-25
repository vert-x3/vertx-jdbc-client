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
import io.vertx.core.json.JsonArray;
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

  public void startTx(Handler<AsyncResult<String>> resultHandler);

  public void startTxWithIsolation(int level, Handler<AsyncResult<String>> resultHandler);

  public void execute(String sql, Handler<AsyncResult<Void>> resultHandler);

  public void executeTx(String txId, String sql, Handler<AsyncResult<Void>> resultHandler);

  public void select(String sql, JsonArray parameters, Handler<AsyncResult<List<JsonObject>>> resultHandler);

  public void selectTx(String txId, String sql, JsonArray parameters, Handler<AsyncResult<List<JsonObject>>> resultHandler);

  public void insert(String sql, JsonArray params, Handler<AsyncResult<JsonObject>> resultHandler);

  public void insertTx(String txId, String sql, JsonArray params, Handler<AsyncResult<JsonObject>> resultHandler);

  //TODO: Insert and update do the same thing really, with insert returning generated keys. We could return JsonObject and just have an update
  public void update(String sql, JsonArray params, Handler<AsyncResult<Integer>> resultHandler);

  public void updateTx(String txId, String sql, JsonArray params, Handler<AsyncResult<Integer>> resultHandler);

  public void commit(String txId, Handler<AsyncResult<Void>> resultHandler);

  public void rollback(String txId, Handler<AsyncResult<Void>> resultHandler);

  @ProxyIgnore
  public void start();

  @ProxyIgnore
  public void stop();
}
