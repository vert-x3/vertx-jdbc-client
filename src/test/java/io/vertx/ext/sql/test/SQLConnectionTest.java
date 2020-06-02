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
package io.vertx.ext.sql.test;

import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.junit.Test;

public class SQLConnectionTest {

  @Test
  public void testAutoCloseable() {
    // No code executed
  }

  // Not executed, we just chech that the statement try-with-resource can be compiled with an SQLConnection
  private void checkAutocloseable(SQLClient client) {
    client.getConnection(ar -> {
      try (SQLConnection conn = ar.result()) {

      }
    });
  }
}
