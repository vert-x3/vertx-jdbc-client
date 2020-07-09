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

import io.vertx.jdbcclient.SqlOutParam;

public class SqlOutParamImpl implements SqlOutParam {

  final Object value;
  final int type;
  final boolean in;

  public SqlOutParamImpl(Object value, int type) {
    this.value = value;
    this.type = type;
    in = true;
  }

  public SqlOutParamImpl(int type) {
    this.value = null;
    this.type = type;
    in = false;
  }

  @Override
  public boolean in() {
    return in;
  }

  @Override
  public int type() {
    return type;
  }

  @Override
  public Object value() {
    return value;
  }
}
