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

package io.vertx.ext.jdbc.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.ServiceHelper;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.ext.jdbc.JdbcService;
import io.vertx.ext.jdbc.RuntimeSqlException;
import io.vertx.ext.jdbc.impl.actions.JdbcCommit;
import io.vertx.ext.jdbc.impl.actions.JdbcExecute;
import io.vertx.ext.jdbc.impl.actions.JdbcInsert;
import io.vertx.ext.jdbc.impl.actions.JdbcRollback;
import io.vertx.ext.jdbc.impl.actions.JdbcSelect;
import io.vertx.ext.jdbc.impl.actions.JdbcStartTx;
import io.vertx.ext.jdbc.impl.actions.JdbcUpdate;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JdbcServiceImpl implements JdbcService {

  private static final Logger log = LoggerFactory.getLogger(JdbcService.class);

  private final Vertx vertx;
  private final JsonObject config;

  //TODO: Connection pool, port mod-jdbc-persistor
  private DataSourceProvider provider;
  private DataSource dataSource;
  private Transactions transactions;

  public JdbcServiceImpl(Vertx vertx, JsonObject config) {
    this.vertx = vertx;
    this.config = config;
    transactions = new Transactions(vertx, config.getInteger("txTimeout", 10000));
  }

  @Override
  public void start() {
    provider = ServiceHelper.loadFactory(DataSourceProvider.class);
    try {
      dataSource = provider.getDataSource(config);
    } catch (SQLException e) {
      throw new RuntimeSqlException(e);
    }
  }

  @Override
  public void stop() {
    if (provider != null) {
      try {
        provider.close(dataSource);
      } catch (SQLException e) {
        log.error("Exception occurred trying to close out the data source", e);
      }
    }
  }

  @Override
  public void startTx(Handler<AsyncResult<String>> resultHandler) {
    new JdbcStartTx(vertx, dataSource, transactions).process(resultHandler);
  }

  @Override
  public void startTxWithIsolation(int level, Handler<AsyncResult<String>> resultHandler) {
    new JdbcStartTx(vertx, dataSource, transactions, level).process(resultHandler);
  }

  @Override
  public void execute(String sql, Handler<AsyncResult<Void>> resultHandler) {
    new JdbcExecute(vertx, dataSource, sql).process(resultHandler);
  }

  @Override
  public void executeTx(String txId, String sql, Handler<AsyncResult<Void>> resultHandler) {
    new JdbcExecute(vertx, transactions, txId, sql).process(resultHandler);
  }

  @Override
  public void select(String sql, JsonArray parameters, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
    new JdbcSelect(vertx, dataSource, sql, parameters).process(resultHandler);
  }

  @Override
  public void selectTx(String txId, String sql, JsonArray parameters, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
    new JdbcSelect(vertx, transactions, txId, sql, parameters).process(resultHandler);
  }

  @Override
  public void insert(String sql, JsonArray parameters, Handler<AsyncResult<JsonObject>> resultHandler) {
    new JdbcInsert(vertx, dataSource, sql, parameters).process(resultHandler);
  }

  @Override
  public void insertTx(String txId, String sql, JsonArray parameters, Handler<AsyncResult<JsonObject>> resultHandler) {
    new JdbcInsert(vertx, transactions, txId, sql, parameters).process(resultHandler);
  }

  @Override
  public void update(String sql, JsonArray params, Handler<AsyncResult<Integer>> resultHandler) {
    new JdbcUpdate(vertx, dataSource, sql, params).process(resultHandler);
  }

  @Override
  public void updateTx(String txId, String sql, JsonArray params, Handler<AsyncResult<Integer>> resultHandler) {
    new JdbcUpdate(vertx, transactions, txId, sql, params).process(resultHandler);
  }

  @Override
  public void delete(String sql, JsonArray params, Handler<AsyncResult<Integer>> resultHandler) {
    new JdbcUpdate(vertx, dataSource, sql, params).process(resultHandler);
  }

  @Override
  public void deleteTx(String txId, String sql, JsonArray params, Handler<AsyncResult<Integer>> resultHandler) {
    new JdbcUpdate(vertx, transactions, txId, sql, params).process(resultHandler);
  }

  @Override
  public void commit(String txId, Handler<AsyncResult<Void>> resultHandler) {
    new JdbcCommit(vertx, transactions, txId).process(resultHandler);
  }

  @Override
  public void rollback(String txId, Handler<AsyncResult<Void>> resultHandler) {
    new JdbcRollback(vertx, transactions, txId).process(resultHandler);
  }
}
