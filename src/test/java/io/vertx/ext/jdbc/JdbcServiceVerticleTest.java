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

import io.vertx.core.DeploymentOptions;

import java.util.concurrent.CountDownLatch;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class JdbcServiceVerticleTest extends JdbcServiceTestBase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    CountDownLatch latch = new CountDownLatch(1);
    vertx.deployVerticle("service:io.vertx:vertx-jdbc-service", new DeploymentOptions().setConfig(config()), onSuccess(id -> {
      service = JdbcService.createEventBusProxy(vertx, "vertx.jdbc");
      latch.countDown();
    }));
    awaitLatch(latch);
  }
}
