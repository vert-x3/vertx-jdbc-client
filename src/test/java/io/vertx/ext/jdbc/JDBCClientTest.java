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

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.impl.actions.AbstractJDBCAction;
import io.vertx.ext.sql.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public class JDBCClientTest extends JDBCClientTestBase {

  protected SQLClient client;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    client = JDBCClient.createNonShared(vertx, config());
  }

  @After
  public void after() throws Exception {
    client.close();
    super.after();
  }

  @Test
  public void testSqlClientInstance() {
    assertTrue(client instanceof SQLClient);
    testComplete();
  }

  @Test
  public void testJdbcClientInstance() {
    assertTrue(client instanceof JDBCClient);
    testComplete();
  }

  @Test
  public void testGetNativeConn() {
    assertNotNull(connection().unwrap());
    testComplete();
  }

  private SQLClient checker() {
    return new CloseConnectionChecker(client, v -> {
      System.out.println("connection closed");
      testComplete();
    });
  }

  @Test
  public void testOneShotStream1() {
    testOneShotStream1(res -> null);
  }

  @Test
  public void testOneShotStream2() {
    testOneShotStream1(res -> v -> res.moreResults());
  }

  @Test
  public void testOneShotStream3() {
    testOneShotStream1(res -> v -> res.close());
  }

  private void testOneShotStream1(Function<SQLRowStream, Handler<Void>> h) {
    final AtomicInteger cnt = new AtomicInteger(0);
    checker().queryStream("SELECT * FROM big_table", onSuccess(res -> {
      res.resultSetClosedHandler(h.apply(res))
        .handler(row -> {
          cnt.incrementAndGet();
        }).endHandler(v -> {
        assertEquals(200, cnt.get());
      }).exceptionHandler(this::fail);
    }));
    await();
  }

  @Test
  public void testOneShotStream4() {
    final AtomicInteger cnt = new AtomicInteger(0);
    checker().queryStream("SELECT * FROM big_table", onSuccess(res -> {
      res.resultSetClosedHandler(v -> {
        fail(new RuntimeException("wrong state"));
      }).handler(row -> {
        if(cnt.incrementAndGet() > 100) {
          res.close();
        }
      }).endHandler(v -> {
        fail(new RuntimeException());
      }).exceptionHandler(t -> {
        fail(t);
      });
    }));
    await();
  }

  @Test
  public void testSelect() {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
    connection().query(sql, onSuccess(resultSet -> {
      assertNotNull(resultSet);
      assertEquals(2, resultSet.getResults().size());
      assertEquals("ID", resultSet.getColumnNames().get(0));
      assertEquals("FNAME", resultSet.getColumnNames().get(1));
      assertEquals("LNAME", resultSet.getColumnNames().get(2));
      JsonArray result0 = resultSet.getResults().get(0);
      assertEquals(1, (int) result0.getInteger(0));
      assertEquals("john", result0.getString(1));
      assertEquals("doe", result0.getString(2));
      JsonArray result1 = resultSet.getResults().get(1);
      assertEquals(2, (int) result1.getInteger(0));
      assertEquals("jane", result1.getString(1));
      assertEquals("doe", result1.getString(2));
      testComplete();
    }));

    await();
  }

  @Test
  public void testSelectOneShot() {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
    client.query(sql, query -> {
      assertFalse(query.failed());
      final ResultSet resultSet = query.result();
      assertNotNull(resultSet);
      assertEquals(2, resultSet.getResults().size());
      assertEquals("ID", resultSet.getColumnNames().get(0));
      assertEquals("FNAME", resultSet.getColumnNames().get(1));
      assertEquals("LNAME", resultSet.getColumnNames().get(2));
      JsonArray result0 = resultSet.getResults().get(0);
      assertEquals(1, (int) result0.getInteger(0));
      assertEquals("john", result0.getString(1));
      assertEquals("doe", result0.getString(2));
      JsonArray result1 = resultSet.getResults().get(1);
      assertEquals(2, (int) result1.getInteger(0));
      assertEquals("jane", result1.getString(1));
      assertEquals("doe", result1.getString(2));
      testComplete();
    });

    await();
  }

  @Test
  public void testSelectOneContext() {
    Context context = vertx.getOrCreateContext();
    context.runOnContext(v -> {
      client.query("VALUES (CURRENT_TIMESTAMP)", onSuccess(query -> {
        assertEquals(context, vertx.getOrCreateContext());
        testComplete();
      }));
    });
    await();
  }

  @Test
  public void testSelectOneShotFail() {
    String sql = "SELECTA ID, FNAME, LNAME FROM select_table ORDER BY ID";
    client.query(sql, query -> {
      assertTrue(query.failed());
      testComplete();
    });

    await();
  }

  @Test
  public void testSelectOneShotSingle() {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table WHERE ID = 2";
    client.querySingle(sql, query -> {
      assertFalse(query.failed());
      final JsonArray row = query.result();
      assertNotNull(row);
      assertEquals(2, (int) row.getInteger(0));
      assertEquals("jane", row.getString(1));
      assertEquals("doe", row.getString(2));
      testComplete();
    });

    await();
  }

  @Test
  public void testStream() {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
    final AtomicInteger cnt = new AtomicInteger(0);
    connection().queryStream(sql, onSuccess(res -> {
      res.resultSetClosedHandler(v -> {
        res.moreResults();
      }).handler(row -> {
        cnt.incrementAndGet();
      }).endHandler(v -> {
        assertEquals(2, cnt.get());
        testComplete();
      }).exceptionHandler(t -> {
        fail(t);
      });
    }));

    await();
  }

  @Test
  public void testStreamOnClosedConnection() {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
    final AtomicInteger cnt = new AtomicInteger(0);
    final SQLConnection conn = connection();

    conn.queryStream(sql, onSuccess(res -> {
      conn.close();

      res.resultSetClosedHandler(v -> {
        fail("Should not happen");
      }).handler(row -> {
        fail("Should not happen");
      }).endHandler(v -> {
        fail("Should not happen");
      }).exceptionHandler(t -> {
        testComplete();
      });
    }));

    await();
  }

  @Test
  public void testStreamWithParams() {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table WHERE LNAME = ? ORDER BY ID";
    final AtomicInteger cnt = new AtomicInteger(0);
    connection().queryStreamWithParams(sql, new JsonArray().add("doe"), onSuccess(res -> {
      res.handler(row -> {
        cnt.incrementAndGet();
      }).endHandler(v -> {
        assertEquals(2, cnt.get());
        testComplete();
      }).exceptionHandler(t -> {
        fail(t);
      });
    }));

    await();
  }

  @Test
  public void testStreamAbort() {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
    connection().queryStream(sql, onSuccess(res -> {
      res.handler(row -> {
        res.close(close -> {
          testComplete();
        });
      }).endHandler(v -> {
        fail("Should not be called");
      }).exceptionHandler(t -> {
        fail(t);
      });
    }));

    await();
  }

  @Test
  public void testStreamPauseResumeFlowControl() {
    testStreamFlowControl(stream -> {
    }, stream -> {
      stream.pause();
      vertx.setTimer(1000, v -> {
        stream.resume();
      });
    });
  }

  @Test
  public void testStreamFetchFlowControl() {
    AtomicBoolean paused = new AtomicBoolean();
    testStreamFlowControl(stream -> {
      stream.pause();
      stream.fetch(1);
    }, stream -> {
      assertFalse(paused.getAndSet(true));
      vertx.setTimer(1000, v -> {
        paused.set(false);
        stream.fetch(1);
      });
    });
  }

  public void testStreamFlowControl(Handler<SQLRowStream> initHandler, Handler<SQLRowStream> dataHandler) {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
    final AtomicInteger cnt = new AtomicInteger(0);
    final long[] t = {0, 0};
    connection().queryStream(sql, onSuccess(res -> {
      res.handler(row -> {
        t[cnt.getAndIncrement()] = System.currentTimeMillis();
        dataHandler.handle(res);
      }).endHandler(v -> {
        assertEquals(2, cnt.get());
        assertTrue(t[1] - t[0] >= 1000);
        testComplete();
      }).exceptionHandler(t0 -> {
        fail(t0);
      });
      initHandler.handle(res);
    }));

    await();
  }

  @Test
  public void testBigStream() {
    String sql = "SELECT * FROM big_table";
    final AtomicInteger cnt = new AtomicInteger(0);
    connection().queryStream(sql, onSuccess(res -> {
      res.resultSetClosedHandler(v -> {
        res.moreResults();
      }).handler(row -> {
        cnt.incrementAndGet();
      }).endHandler(v -> {
        assertEquals(200, cnt.get());
        testComplete();
      }).exceptionHandler(t -> {
        fail(t);
      });
    }));

    await();
  }

  @Test
  public void testStreamColumnResolution() {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
    final AtomicInteger cnt = new AtomicInteger(0);
    connection().queryStream(sql, onSuccess(res -> {
      res.handler(row -> {
        assertEquals("doe", row.getString(res.column("lname")));
        cnt.incrementAndGet();
      }).endHandler(v -> {
        assertEquals(2, cnt.get());
        testComplete();
      }).exceptionHandler(t -> {
        fail(t);
      });
    }));

    await();
  }

  @Test
  public void testStreamGetColumns() {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
    connection().queryStream(sql, onSuccess(res -> {
      assertEquals(Arrays.asList("ID", "FNAME", "LNAME"), res.columns());
      // assert the collection is immutable
      try {
        res.columns().add("durp!");
        fail();
      } catch (RuntimeException e) {
        // expected!
      }
      testComplete();
    }));

    await();
  }


  @Test
  public void testSelectWithParameters() {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table WHERE fname = ?";

    connection().queryWithParams(sql, new JsonArray().add("john"), onSuccess(resultSet -> {
      assertNotNull(resultSet);
      assertEquals(1, resultSet.getResults().size());
      assertEquals("ID", resultSet.getColumnNames().get(0));
      assertEquals("FNAME", resultSet.getColumnNames().get(1));
      assertEquals("LNAME", resultSet.getColumnNames().get(2));
      JsonArray result0 = resultSet.getResults().get(0);
      assertEquals(1, (int) result0.getInteger(0));
      assertEquals("john", result0.getString(1));
      assertEquals("doe", result0.getString(2));
      testComplete();
    }));

    await();
  }

  @Test
  public void testSelectWithLabels() {
    String sql = "SELECT ID as \"IdLabel\", FNAME as \"first_name\", LNAME as \"LAST.NAME\" FROM select_table WHERE fname = ?";

    connection().queryWithParams(sql, new JsonArray().add("john"), onSuccess(resultSet -> {
      assertNotNull(resultSet);
      assertEquals(1, resultSet.getResults().size());
      assertEquals("IdLabel", resultSet.getColumnNames().get(0));
      assertEquals("first_name", resultSet.getColumnNames().get(1));
      assertEquals("LAST.NAME", resultSet.getColumnNames().get(2));
      JsonArray result0 = resultSet.getResults().get(0);
      assertEquals(1, (int) result0.getInteger(0));
      assertEquals("john", result0.getString(1));
      assertEquals("doe", result0.getString(2));
      JsonObject row0 = resultSet.getRows().get(0);
      assertEquals(1, (int) row0.getInteger("IdLabel"));
      assertEquals("john", row0.getString("first_name"));
      assertEquals("doe", row0.getString("LAST.NAME"));
      testComplete();
    }));

    await();
  }

  @Test
  public void testSelectTx() {
    String sql = "INSERT INTO insert_table VALUES (?, ?, ?, ?);";
    JsonArray params = new JsonArray().addNull().add("smith").add("john").add("2003-03-03");
    client.getConnection(onSuccess(conn -> {
      assertNotNull(conn);
      conn.setAutoCommit(false, onSuccess(v -> {

        conn
          .setOptions(new SQLOptions().setAutoGeneratedKeys(true))
          .updateWithParams(sql, params, onSuccess((UpdateResult updateResult) -> {
            assertUpdate(updateResult, 1);
            int id = updateResult.getKeys().getInteger(0);
            // Explicit typing of resultset is not really necessary but without it IntelliJ reports
            // syntax error :(
            conn.queryWithParams("SELECT LNAME FROM insert_table WHERE id = ?", new JsonArray().add(id), onSuccess((ResultSet resultSet) -> {
              assertFalse(resultSet.getResults().isEmpty());
              assertEquals("smith", resultSet.getResults().get(0).getString(0));
              testComplete();
            }));
          }));
      }));
    }));

    await();
  }

  @Test
  public void testInvalidSelect() {
    // Suppress log output so this test doesn't look to fail
    setLogLevel(AbstractJDBCAction.class.getName(), Level.SEVERE);
    String sql = "SELECT FROM WHERE FOO BAR";
    connection().query(sql, onFailure(t -> {
      assertNotNull(t);
      testComplete();
    }));

    await();
  }

  @Test
  public void testInsert() {
    String sql = "INSERT INTO insert_table VALUES (null, 'doe', 'john', '2001-01-01');";
    connection().update(sql, onSuccess(result -> {
      assertUpdate(result, 1);
      testComplete();
    }));

    await();
  }

  @Test
  public void testNaturalInsert() {
    String sql = "INSERT INTO insert_table2 VALUES (1, 'doe', 'john', '2001-01-01');";
    connection().update(sql, onSuccess(result -> {
      assertUpdate(result, 1);
      testComplete();
    }));

    await();
  }

  @Test
  public void testInsertWithParameters() {
    final TimeZone tz = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    SQLConnection conn = connection();
    String sql = "INSERT INTO insert_table VALUES (?, ?, ?, ?);";
    JsonArray params = new JsonArray().addNull().add("doe").add("jane").add("2002-02-02");
    conn
      .setOptions(new SQLOptions().setAutoGeneratedKeys(true))
      .updateWithParams(sql, params, onSuccess(result -> {
        assertUpdate(result, 1);
        int id = result.getKeys().getInteger(0);
        conn.queryWithParams("SElECT DOB FROM insert_table WHERE id=?;", new JsonArray().add(id), onSuccess(resultSet -> {
          assertNotNull(resultSet);
          assertEquals(1, resultSet.getResults().size());
          assertEquals("2002-02-02", resultSet.getResults().get(0).getString(0));
          TimeZone.setDefault(tz);
          testComplete();
        }));
      }));

    await();
  }

  @Test
  public void testUpdate() {
    SQLConnection conn = connection();
    String sql = "UPDATE update_table SET fname='jane' WHERE id = 1";
    conn.update(sql, onSuccess(updated -> {
      assertUpdate(updated, 1);
      conn.query("SELECT fname FROM update_table WHERE id = 1", onSuccess(resultSet -> {
        assertNotNull(resultSet);
        assertEquals(1, resultSet.getResults().size());
        assertEquals("jane", resultSet.getResults().get(0).getString(0));
        testComplete();
      }));
    }));

    await();
  }

  @Test
  public void testUpdateWithParams() {
    SQLConnection conn = connection();
    String sql = "UPDATE update_table SET fname = ? WHERE id = ?";
    JsonArray params = new JsonArray().add("bob").add(1);
    conn.updateWithParams(sql, params, onSuccess(result -> {
      assertUpdate(result, 1);
      conn.query("SELECT fname FROM update_table WHERE id = 1", onSuccess(resultSet -> {
        assertNotNull(resultSet);
        assertEquals(1, resultSet.getResults().size());
        assertEquals("bob", resultSet.getResults().get(0).getString(0));
        testComplete();
      }));
    }));

    await();
  }

  @Test
  public void testUpdateNoMatch() {
    SQLConnection conn = connection();
    String sql = "UPDATE update_table SET fname='jane' WHERE id = -231";
    conn.update(sql, onSuccess(result -> {
      assertUpdate(result, 0);
      testComplete();
    }));

    await();
  }

  @Test
  public void testDelete() {
    String sql = "DELETE FROM delete_table WHERE id = 1;";
    connection().update(sql, onSuccess(result -> {
      assertNotNull(result);
      assertEquals(1, result.getUpdated());
      testComplete();
    }));

    await();
  }

  @Test
  public void testDeleteWithParams() {
    String sql = "DELETE FROM delete_table WHERE id = ?;";
    JsonArray params = new JsonArray().add(2);
    connection().updateWithParams(sql, params, onSuccess(result -> {
      assertNotNull(result);
      assertEquals(1, result.getUpdated());
      testComplete();
    }));

    await();
  }

  @Test
  public void testClose() throws Exception {
    client.getConnection(onSuccess(conn -> {
      conn.query("SELECT 1 FROM select_table", onSuccess(results -> {
        assertNotNull(results);
        conn.close(onSuccess(v -> {
          testComplete();
        }));
      }));
    }));

    await();
  }

  @Test
  public void testCloseThenQuery() throws Exception {
    client.getConnection(onSuccess(conn -> {
      conn.close(onSuccess(v -> {
        conn.query("SELECT 1 FROM select_table", onFailure(t -> {
          assertNotNull(t);
          testComplete();
        }));
      }));
    }));

    await();
  }

  @Test
  public void testCommit() throws Exception {
    testTx(3, true);
  }

  @Test
  public void testRollback() throws Exception {
    testTx(5, false);
  }

  @Test
  public void testBlob() {
    String sql = "SELECT b FROM blob_table";
    connection().query(sql, onSuccess(resultSet -> {
      assertNotNull(resultSet);
      assertEquals(1, resultSet.getResults().size());
      assertNotNull(resultSet.getResults().get(0).getBinary(0));
      testComplete();
    }));

    await();
  }

  @Test
  public void testClob() {
    String sql = "SELECT c FROM blob_table";
    connection().query(sql, onSuccess(resultSet -> {
      assertNotNull(resultSet);
      assertEquals(1, resultSet.getResults().size());
      assertNotNull(resultSet.getResults().get(0).getString(0));
      testComplete();
    }));

    await();
  }

  @Test
  public void testArray() {
    String sql = "SELECT a FROM blob_table";
    connection().query(sql, onSuccess(resultSet -> {
      assertNotNull(resultSet);
      assertEquals(1, resultSet.getResults().size());
      assertNotNull(resultSet.getResults().get(0).getJsonArray(0));
      testComplete();
    }));

    await();
  }

  @Test
  public void testWorkerPerConnection() {
    int numConns = 4;
    ArrayList<SQLConnection> conns = new ArrayList<>();
    for (int i = 0; i < numConns; i++) {
      conns.add(connection());
    }
    AtomicInteger count = new AtomicInteger();
    Context context = vertx.getOrCreateContext();
    context.runOnContext(v -> {
      for (SQLConnection conn : conns) {
        conn.setAutoCommit(false, onSuccess(ar1 -> {
          conn.execute("LOCK TABLE insert_table WRITE", onSuccess(ar2 -> {
            String sql = "INSERT INTO insert_table VALUES (null, 'doe', 'john', '2001-01-01');";
            conn.update(sql, onSuccess(res3 -> {
              conn.commit(onSuccess(committed -> {
                conn.close(onSuccess(closed -> {
                  if (count.incrementAndGet() == numConns) {
                    testComplete();
                  }
                }));
              }));
            }));
          }));
        }));
      }
    });
    await();
  }

  @Test
  public void testSameContext() {
    Context ctx = vertx.getOrCreateContext();
    SQLConnection conn = connection(ctx);
    conn.query("SELECT a FROM blob_table", onSuccess(rs -> {
      assertSame(Vertx.currentContext(), ctx);
      testComplete();
    }));
    await();
  }

  private void testTx(int inserts, boolean commit) throws Exception {
    String sql = "INSERT INTO insert_table VALUES (?, ?, ?, ?);";
    JsonArray params = new JsonArray().addNull().add("smith").add("john").add("2003-03-03");
    List<Integer> insertIds = new CopyOnWriteArrayList<>();

    CountDownLatch latch = new CountDownLatch(inserts);
    AtomicReference<SQLConnection> connRef = new AtomicReference<>();
    client.getConnection(onSuccess(conn -> {
      assertNotNull(conn);
      connRef.set(conn);
      conn.setAutoCommit(false, onSuccess(v -> {
        for (int i = 0; i < inserts; i++) {
          // Explicit typing of UpdateResult is not really necessary but without it IntelliJ reports
          // syntax error :(
          conn
            .setOptions(new SQLOptions().setAutoGeneratedKeys(true))
            .updateWithParams(sql, params, onSuccess((UpdateResult result) -> {
              assertUpdate(result, 1);
              int id = result.getKeys().getInteger(0);
              insertIds.add(id);
              latch.countDown();
            }));
        }
      }));
    }));

    awaitLatch(latch);

    StringBuilder selectSql = new StringBuilder("SELECT * FROM insert_table WHERE");
    JsonArray selectParams = new JsonArray();
    for (int i = 0; i < insertIds.size(); i++) {
      selectParams.add(insertIds.get(i));
      if (i == 0) {
        selectSql.append(" id = ?");
      } else {
        selectSql.append(" OR id = ?");
      }
    }

    SQLConnection conn = connRef.get();
    if (commit) {
      conn.commit(onSuccess(v -> {
        client.getConnection(onSuccess(newconn -> {
          // Explicit typing of resultset is not really necessary but without it IntelliJ reports
          // syntax error :(
          newconn.queryWithParams(selectSql.toString(), selectParams, onSuccess((ResultSet resultSet) -> {
            assertEquals(inserts, resultSet.getResults().size());
            testComplete();
          }));
        }));
      }));
    } else {
      conn.rollback(onSuccess(v -> {
        client.getConnection(onSuccess(newconn -> {
          // Explicit typing of resultset is not really necessary but without it IntelliJ reports
          // syntax error :(
          newconn.queryWithParams(selectSql.toString(), selectParams, onSuccess((ResultSet resultSet) -> {
            assertTrue(resultSet.getResults().isEmpty());
            testComplete();
          }));
        }));
      }));
    }

    await();
  }

  protected SQLConnection connection() {
    return connection(vertx.getOrCreateContext());
  }

  protected SQLConnection connection(Context context) {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<SQLConnection> ref = new AtomicReference<>();
    context.runOnContext(v -> {
      client.getConnection(onSuccess(conn -> {
        ref.set(conn);
        latch.countDown();
      }));
    });

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return ref.get();
  }
}
