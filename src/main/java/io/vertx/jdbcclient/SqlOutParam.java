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
package io.vertx.jdbcclient;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.jdbcclient.impl.SqlOutParamImpl;

import java.sql.JDBCType;

/**
 * Tag if a parameter is of type OUT or INOUT.
 *
 * By default parameters are of type IN as they are provided by the user to the RDBMs engine. There are however cases
 * where these must be tagged as OUT/INOUT when dealing with stored procedures/functions or complex statements.
 *
 * This interface allows marking the type of the param as required by the JDBC API.
 */
@VertxGen
public interface SqlOutParam {

  /**
   * Factory for a OUT parameter of type {@code out}.
   * @param out the kind of the type according to JDBC types.
   * @return new marker
   */
  static SqlOutParam OUT(int out) {
    return new SqlOutParamImpl(out);
  }

  /**
   * Factory for a OUT parameter of type {@code out}.
   * @param out the kind of the type according to JDBC types.
   * @return new marker
   */
  static SqlOutParam OUT(String out) {
    return new SqlOutParamImpl(JDBCType.valueOf(out).getVendorTypeNumber());
  }

  /**
   * Factory for a OUT parameter of type {@code out}.
   * @param out the kind of the type according to JDBC types.
   * @return new marker
   */
  static SqlOutParam OUT(JDBCType out) {
    return new SqlOutParamImpl(out.getVendorTypeNumber());
  }

  /**
   * Factory for a INOUT parameter of type {@code out}.
   * @param in the value to be passed as input.
   * @param out the kind of the type according to JDBC types.
   * @return new marker
   */
  static SqlOutParam INOUT(Object in, int out) {
    return new SqlOutParamImpl(in, out);
  }

  /**
   * Factory for a INOUT parameter of type {@code out}.
   * @param in the value to be passed as input.
   * @param out the kind of the type according to JDBC types.
   * @return new marker
   */
  static SqlOutParam INOUT(Object in, String out) {
    return new SqlOutParamImpl(in, JDBCType.valueOf(out).getVendorTypeNumber());
  }

  /**
   * Factory for a INOUT parameter of type {@code out}.
   * @param in the value to be passed as input.
   * @param out the kind of the type according to JDBC types.
   * @return new marker
   */
  static SqlOutParam INOUT(Object in, JDBCType out) {
    return new SqlOutParamImpl(in, out.getVendorTypeNumber());
  }

  /**
   * Is this marker {@code IN}?
   * @return true if {@code INOUT}
   */
  boolean in();

  /**
   * Get the output type
   * @return type
   */
  int type();

  /**
   * Get the input value
   * @return input
   */
  Object value();
}
