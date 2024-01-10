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
package io.vertx.jdbcclient.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.CloseFuture;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;
import io.vertx.ext.jdbc.impl.actions.JDBCStatementHelper;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.impl.PoolImpl;
import io.vertx.sqlclient.impl.SqlConnectionBase;

import java.sql.Connection;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JDBCPoolImpl extends PoolImpl implements JDBCPool {

  private final VertxInternal vertx;

  private static class ConnectionFactory {

    private static final String NET_LOCATION_REGEX = "(?<netloc>[0-9.]+|\\[[a-zA-Z0-9:]+]|[a-zA-Z0-9\\-._~%]+)"; // ip v4/v6 address, host, domain socket address
    private static final String PORT_REGEX = "(:(?<port>\\d+))?"; // port
    private static final Pattern HOST_AND_PORT_PATTERN = Pattern.compile("://" + NET_LOCATION_REGEX + PORT_REGEX);

    private final VertxInternal vertx;
    private final JDBCConnectOptions sqlOptions;
    private final Callable<Connection> connectionFactory;

    public ConnectionFactory(VertxInternal vertx, JDBCConnectOptions sqlOptions, Callable<Connection> connectionFactory) {
      this.vertx = vertx;
      this.sqlOptions = sqlOptions;
      this.connectionFactory = connectionFactory;
    }

    private SocketAddress getServer(Connection conn) throws Exception {
      String url = conn.getMetaData().getURL();
      Matcher match = HOST_AND_PORT_PATTERN.matcher(url);
      if (match.find()) {
        String host = parseNetLocation(match.group("netloc"));
        int port;
        String portString;
        if (match.groupCount() > 1 && (portString = match.group("port")) != null && portString.length() > 0) {
          port = Integer.parseInt(portString);
        } else {
          port = 0;
        }
        return SocketAddress.inetSocketAddress(port, host);
      } else  {
        // Use a dummy address when cannot guess one
        return SocketAddress.inetSocketAddress(1234, "unknown");
      }
    }

    private static String parseNetLocation(String host) {
      if (isRegardedAsIpv6Address(host)) {
        return host.substring(1, host.length() - 1);
      } else {
        return host;
      }
    }

    private static boolean isRegardedAsIpv6Address(String address) {
      return address.startsWith("[") && address.endsWith("]");
    }

    public Future<SqlConnection> connect(ContextInternal context) {
      JsonObject cfg = sqlOptions.getExtraConfig();
      if (cfg == null) {
        cfg = new JsonObject();
      }
      JDBCStatementHelper helper = new JDBCStatementHelper(cfg);
      return context.executeBlockingInternal(() -> {
        Connection conn = connectionFactory.call();
        VertxMetrics vertxMetrics = vertx.metricsSPI();
        SocketAddress server = getServer(conn);
        ClientMetrics metrics = vertxMetrics != null ? vertxMetrics.createClientMetrics(server, "sql", null) : null;
        return new SqlConnectionBase<>(context, null, new ConnectionImpl(helper, context, sqlOptions, conn, metrics, sqlOptions.getUser(), sqlOptions.getDatabase(), server), FakeDriver.INSTANCE);
      });
    }
  }

  public JDBCPoolImpl(Vertx vertx,
                      JDBCConnectOptions sqlOptions,
                      Callable<Connection> connectionFactory,
                      PoolOptions poolOptions,
                      CloseFuture closeFuture) {
    super((VertxInternal) vertx,
      FakeDriver.INSTANCE,
      false,
      poolOptions,
      null,
      null,
      ctx -> new ConnectionFactory((VertxInternal) vertx, sqlOptions, connectionFactory).connect((ContextInternal) ctx),
      null,
      closeFuture);
    this.vertx = (VertxInternal) vertx;
  }

  @Override
  protected ContextInternal context() {
    return vertx.getOrCreateContext();
  }

  @Override
  protected <T> PromiseInternal<T> promise() {
    return vertx.promise();
  }

  @Override
  public Future<Void> close() {
    return super.close();
  }
}
