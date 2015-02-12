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

import io.vertx.core.AbstractVerticle;
import io.vertx.serviceproxy.ProxyHelper;

/**
 * A verticle that starts an instance of a JDBC service
 *
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JdbcServiceVerticle extends AbstractVerticle {

  protected JdbcService service;

  @Override
  public void start() throws Exception {
    service = JdbcService.create(vertx, config());

    // And register it on the event bus against the configured address
    String address = config().getString("address");
    if (address == null) {
      throw new IllegalStateException("address field must be specified in config for service verticle");
    }
    ProxyHelper.registerService(JdbcService.class, vertx, service, address);

    // Start it
    service.start();
  }

  @Override
  public void stop() throws Exception {
    if (service != null) {
      service.stop();
    }
  }
}
