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

import io.vertx.core.Vertx;
import io.vertx.ext.jdbc.impl.Transactions;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JdbcStartTx extends AbstractJdbcAction<String> {

  private final Transactions transactions;
  private final Integer isolationLevel;

  public JdbcStartTx(Vertx vertx, DataSource dataSource, Transactions transactions) {
    this(vertx, dataSource, transactions, null);
  }

  public JdbcStartTx(Vertx vertx, DataSource dataSource, Transactions transactions, Integer isolationLevel) {
    this(vertx, dataSource, transactions, UUID.randomUUID().toString(), isolationLevel);
  }

  public JdbcStartTx(Vertx vertx, DataSource dataSource, Transactions transactions, String txId, Integer isolationLevel) {
    super(vertx, connection(dataSource));
    this.transactions = transactions;
    this.txId = txId;
    this.isolationLevel = isolationLevel;
  }

  @Override
  protected String execute(Connection conn) throws SQLException {
    conn.setAutoCommit(false);
    if (isolationLevel != null) {
      conn.setTransactionIsolation(isolationLevel);
    }
    Connection prev = transactions.putIfAbsent(txId, conn);
    if (prev != null) throw new IllegalStateException("Possible duplicate transaction " + txId);

    return txId;
  }

  @Override
  protected String name() {
    return "startTx";
  }
}
