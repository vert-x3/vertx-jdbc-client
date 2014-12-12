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

package examples.io.vertx.ext.jdbc;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JdbcService;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 */
public class Examples {

  private Vertx vertx;

  public void deployJdbcService() {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle("service:io.vertx:vertx-jdbc");
  }

  public void deployJdbcServiceWithConfig() {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle("service:io.vertx:vertx-jdbc",
      new DeploymentOptions().setConfig(new JsonObject().put("url", "jdbc:postgresql://localhost/vertx?user=postgres")));
  }

  public void eventBusProxy() {
    JdbcService service = JdbcService.createEventBusProxy(vertx, "vertx.jdbc");
  }
}
