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

package io.vertx.jdbcclient.impl.actions;

import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.jdbcclient.SqlOptions;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JDBCClose extends AbstractJDBCAction<Void> {

  private final PoolMetrics metrics; // the pool metrics
  private final Object metric;       // the resource managed by the pool metrics

  public JDBCClose(SqlOptions options, PoolMetrics metrics, Object metric) {
    super(options);
    this.metrics = metrics;
    this.metric = metric;
  }

  @Override
  public Void execute(Connection conn) throws SQLException {
    if (!conn.isClosed()) {
      if (metrics != null) {
        metrics.end(metric);
      }
      conn.close();
    }
    return null;
  }

}
