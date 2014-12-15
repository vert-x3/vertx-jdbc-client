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
import io.vertx.serviceproxy.ProxyHelper;

/**
 * The JDBC Service is responsible for obtaining either a <code>JdbcConnection</code> or <code>JdbcTransaction</code>
 * which can be used to pass SQL statements to a JDBC driver.
 *
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

  /**
   * Returns a connection that can be used to perform SQL operations on. It's important to remember
   * to close the connection when you are done, so it is returned to the pool.
   *
   * @param handler the handler which is called when the <code>JdbcConnection</code> object is ready for use.
   */
  void getConnection(Handler<AsyncResult<JdbcConnection>> handler);

  /**
   * Normally invoked by the <code>JdbcServiceVerticle</code> to start the service when deployed.
   * This is usually not called by the user.
   */
  @ProxyIgnore
  public void start();

  /**
   * Normally invoked by the <code>JdbcServiceVerticle</code> to stop the service when the verticle is stopped/undeployed.
   * This is usually not called by the user.
   */
  @ProxyIgnore
  public void stop();
}
