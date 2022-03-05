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

package io.vertx.it;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

@RunWith(VertxUnitRunner.class)
public class PgSQLTest {
  @ClassRule
  public static final RunTestOnContext rule = new RunTestOnContext();

  @SuppressWarnings("rawtypes")
  @ClassRule
  public static final PostgreSQLContainer server = (PostgreSQLContainer) new PostgreSQLContainer("postgres:10")
    .withInitScript("init-pgsql.sql");

  private JDBCClient initJDBCClient() {
    JsonObject options = new JsonObject()
      .put("url", server.getJdbcUrl())
      .put("user", server.getUsername())
      .put("password", server.getPassword())
      .put("driver_class", "org.postgresql.Driver");
    return JDBCClient.createShared(rule.vertx(), options, "pgTest");
  }

  @Test
  public void testQueryTemporalTable(TestContext should) {
    final Async async = should.async();
    JDBCClient client = initJDBCClient();
    client
      .query("SELECT * FROM temporal_data_type WHERE id = 1", should.asyncAssertSuccess(resultSet -> {
        Assert.assertEquals(1, resultSet.getNumRows());
        final JsonArray row = resultSet.getResults().get(0);
        Assert.assertEquals(1, (int) row.getInteger(0));
        Assert.assertEquals(LocalDate.parse("1981-05-30"), row.getValue(1));
        Assert.assertEquals(LocalTime.parse("17:55:04.905120000"), row.getValue(2));
        Assert.assertEquals(OffsetTime.parse("14:48:04.905Z"), row.getValue(3));
        Assert.assertEquals(LocalDateTime.parse("2017-05-14T19:35:58.237666"), row.getValue(4));
        Assert.assertEquals(OffsetDateTime.parse("2017-05-15T02:59:59.237666000Z"), row.getValue(5));
        Assert.assertEquals("10 years 3 mons 332 days 20 hours 20 mins 20.999999 secs", row.getValue(6));
        async.complete();
      }));
  }
}
