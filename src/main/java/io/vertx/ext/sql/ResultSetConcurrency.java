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
package io.vertx.ext.sql;

import io.vertx.codegen.annotations.VertxGen;

import java.sql.ResultSet;

/**
 * Represents the resultset concurrency hint
 *
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
@VertxGen
public enum ResultSetConcurrency {

  READ_ONLY(ResultSet.CONCUR_READ_ONLY),
  UPDATABLE(ResultSet.CONCUR_UPDATABLE);

  private final int type;

  ResultSetConcurrency(int type) {
    this.type = type;
  }

  public int getType() {
    return type;
  }
}
