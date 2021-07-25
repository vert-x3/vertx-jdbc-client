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
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.JDBCDecoder;
import io.vertx.ext.jdbc.spi.impl.JDBCDecoderImpl;

import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.TimeZone;

import static java.time.format.DateTimeFormatter.*;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public final class JDBCStatementHelper {

  private static final Logger log = LoggerFactory.getLogger(JDBCStatementHelper.class);

  private static final JsonArray EMPTY = new JsonArray(Collections.unmodifiableList(new ArrayList<>()));

  private final boolean castUUID;
  private final boolean castDate;
  private final boolean castTime;
  private final boolean castDatetime;
  private final JDBCDecoder decoder;

  public JDBCStatementHelper() {
    this(new JsonObject());
  }

  public JDBCStatementHelper(JsonObject config) {
    this.castUUID = config.getBoolean("castUUID", false);
    this.castDate = config.getBoolean("castDate", true);
    this.castTime = config.getBoolean("castTime", true);
    this.castDatetime = config.getBoolean("castDatetime", true);
    this.decoder = Optional.ofNullable(ServiceHelper.loadFactoryOrNull(JDBCDecoder.class)).orElseGet(JDBCDecoderImpl::new);
  }

  public JDBCDecoder getDecoder() {
    return decoder;
  }

  public void fillStatement(PreparedStatement statement, JsonArray in) throws SQLException {
    if (in == null) {
      in = EMPTY;
    }

    for (int i = 0; i < in.size(); i++) {
      Object value = in.getValue(i);

      if (value != null) {
        if (value instanceof String) {
          statement.setObject(i + 1, optimisticCast((String) value));
        } else {
          statement.setObject(i + 1, value);
        }
      } else {
        statement.setObject(i + 1, null);
      }
    }
  }

  public void fillStatement(CallableStatement statement, JsonArray in, JsonArray out) throws SQLException {
    if (in == null) {
      in = EMPTY;
    }

    if (out == null) {
      out = EMPTY;
    }

    int max = Math.max(in.size(), out.size());

    for (int i = 0; i < max; i++) {
      Object value = null;
      boolean set = false;

      if (i < in.size()) {
        value = in.getValue(i);
      }

      // found a in value, use it as a input parameter
      if (value != null) {
        if (value instanceof String) {
          statement.setObject(i + 1, optimisticCast((String) value));
        } else {
          statement.setObject(i + 1, value);
        }
        set = true;
      }

      // reset
      value = null;

      if (i < out.size()) {
        value = out.getValue(i);
      }

      // found a out value, use it as a output parameter
      if (value != null) {
        // We're using the int from the enum instead of the enum itself to allow working with Drivers
        // that have not been upgraded to Java8 yet.
        if (value instanceof String) {
          statement.registerOutParameter(i + 1, JDBCType.valueOf((String) value).getVendorTypeNumber());
        } else if (value instanceof Number) {
          // for cases where vendors have special codes (e.g.: Oracle)
          statement.registerOutParameter(i + 1, ((Number) value).intValue());
        }
        set = true;
      }

      if (!set) {
        // assume null input
        statement.setNull(i + 1, Types.NULL);
      }
    }
  }

  public Object optimisticCast(String value) {
    if (value == null) {
      return null;
    }

    try {
      // sql time
      if (castTime && JDBCDecoder.TIME.matcher(value).matches()) {
        // convert from local time to instant
        Instant instant = LocalTime.parse(value).atDate(LocalDate.of(1970, 1, 1)).toInstant(ZoneOffset.UTC);
        // calculate the timezone offset in millis
        int offset = TimeZone.getDefault().getOffset(instant.toEpochMilli());
        // need to remove the offset since time has no TZ component
        return new Time(instant.toEpochMilli() - offset);
      }

      // sql date
      if (castDate && JDBCDecoder.DATE.matcher(value).matches()) {
        // convert from local date to instant
        Instant instant = LocalDate.parse(value).atTime(LocalTime.of(0, 0, 0, 0)).toInstant(ZoneOffset.UTC);
        // calculate the timezone offset in millis
        int offset = TimeZone.getDefault().getOffset(instant.toEpochMilli());
        // need to remove the offset since time has no TZ component
        return new Date(instant.toEpochMilli() - offset);
      }

      // sql timestamp
      if (castDatetime && JDBCDecoder.DATETIME.matcher(value).matches()) {
        Instant instant = Instant.from(ISO_INSTANT.parse(value));
        return Timestamp.from(instant);
      }

      // sql uuid
      if (castUUID && JDBCDecoder.UUID.matcher(value).matches()) {
        return java.util.UUID.fromString(value);
      }

    } catch (RuntimeException e) {
      log.debug(e);
    }

    // unknown
    return value;
  }
}
