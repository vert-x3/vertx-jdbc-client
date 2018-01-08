/*
 * Copyright 2018 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.vertx.ext.jdbc.spi.impl;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.*;

/**
 * @author Thomas Segismont
 */
public class C3P0DataSourceProviderTest extends VertxTestBase {

  private SQLClient client;

  @Override
  protected void tearDown() throws Exception {
    client.close();
    super.tearDown();
  }

  private JsonObject config() {
    return new JsonObject()
      .put("url", "jdbc:hsqldb:hsqls://zoom.zoom.zen.tld/doesnotexist")
      .put("driver_class", "org.hsqldb.jdbcDriver");
  }

  @Test
  public void continuingConnectionAttempts() {
    client = JDBCClient.createNonShared(vertx, config());
    vertx.setTimer(2000, res -> {
      testComplete();
    });
    client.getConnection(ar -> {
      fail("Should not get invoked");
    });
    await();
  }

  @Test
  public void stopConnectionAttempts() {
    JsonObject config = config().put("acquire_retry_attempts", 1).put("break_after_acquire_failure", true);
    client = JDBCClient.createNonShared(vertx, config);
    vertx.setTimer(2000, res -> {
      fail("Should not get invoked");
    });
    client.getConnection(onFailure(t -> {
      assertThat(t, instanceOf(SQLException.class));
      testComplete();
    }));
    await();
  }
}
