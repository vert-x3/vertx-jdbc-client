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

package io.vertx.ext.jdbc.impl.actions;

import io.vertx.core.ServiceHelper;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.JDBCDecoder;
import io.vertx.ext.jdbc.spi.JDBCEncoder;
import io.vertx.ext.jdbc.spi.impl.JDBCDecoderImpl;
import io.vertx.ext.jdbc.spi.impl.JDBCEncoderImpl;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
//FIXME Update document
//- Remove some config properties: castUUID/castDate/castDateTime/castTime
//- Add 2 new properties: encoderCls and decoderCls
public final class JDBCStatementHelper {

  public static final Pattern UUID = Pattern.compile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");

  private final JDBCEncoder encoder;
  private final JDBCDecoder decoder;

  public JDBCStatementHelper() {
    this(new JsonObject());
  }

  public JDBCStatementHelper(JsonObject config) {
    this.encoder = initEncoder(config);
    this.decoder = initDecoder(config);
  }

  private JDBCEncoder initEncoder(JsonObject config) {
    JDBCEncoder encoder = initObject(config.getString("encoderCls"));
    if (encoder == null) {
      encoder = Optional.ofNullable(ServiceHelper.loadFactoryOrNull(JDBCEncoder.class)).orElseGet(JDBCEncoderImpl::new);
    }
    return encoder;
  }

  private JDBCDecoder initDecoder(JsonObject config) {
    JDBCDecoder decoder = initObject(config.getString("decoderCls"));
    if (decoder == null) {
      return Optional.ofNullable(ServiceHelper.loadFactoryOrNull(JDBCDecoder.class)).orElseGet(JDBCDecoderImpl::new);
    }
    return decoder;
  }

  public JDBCEncoder getEncoder() {
    return encoder;
  }

  public JDBCDecoder getDecoder() {
    return decoder;
  }

  private static <T> T initObject(String clsName) {
    Class<T> cls = findClass(clsName);
    if (cls == null) {
      return null;
    }
    try {
      return cls.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot instantiate " + cls.getName(), e);
    }
  }

  private static <T> Class<T> findClass(String cls) {
    if (Objects.isNull(cls)) {
      return null;
    }
    for (ClassLoader classLoader : Arrays.asList(Thread.currentThread().getContextClassLoader(), JDBCStatementHelper.class.getClassLoader())) {
      try {
        return (Class<T>) Class.forName(cls, true, classLoader);
      } catch (ClassNotFoundException e) {
        //ignore
      } catch (ClassCastException e) {
        return null;
      }
    }
    return null;
  }

}
