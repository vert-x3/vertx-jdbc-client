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

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.junit.*;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNotNull;

@RunWith(VertxUnitRunner.class)
public class JDBCPoolTest extends ClientTestBase {

  private static final JDBCConnectOptions options = new JDBCConnectOptions()
    .setJdbcUrl("jdbc:hsqldb:mem:" + JDBCPoolTest.class.getSimpleName() + "?shutdown=true");

  private static final List<String> SQL = new ArrayList<>();

  static {
    System.setProperty("textdb.allow_full_path", "true");
    //TODO: Create table with more types for testing
    SQL.add("drop table if exists select_table;");
    SQL.add("drop table if exists insert_table;");
    SQL.add("drop table if exists insert_table2;");
    SQL.add("drop table if exists update_table;");
    SQL.add("drop table if exists delete_table;");
    SQL.add("drop table if exists blob_table;");
    SQL.add("drop table if exists big_table;");
    SQL.add("create table select_table (id int, lname varchar(255), fname varchar(255) );");
    SQL.add("insert into select_table values (1, 'doe', 'john');");
    SQL.add("insert into select_table values (2, 'doe', 'jane');");
    SQL.add("create table insert_table (id int generated by default as identity (start with 2 increment by 2) not null, lname varchar(255), fname varchar(255), dob date );");
    SQL.add("create table insert_table2 (id int not null, lname varchar(255), fname varchar(255), dob date );");
    SQL.add("create table update_table (id int, lname varchar(255), fname varchar(255), dob date );");
    SQL.add("insert into update_table values (1, 'doe', 'john', '2001-01-01');");
    SQL.add("create table delete_table (id int, lname varchar(255), fname varchar(255), dob date );");
    SQL.add("insert into delete_table values (1, 'doe', 'john', '2001-01-01');");
    SQL.add("insert into delete_table values (2, 'doe', 'jane', '2002-02-02');");
    SQL.add("create table blob_table (b blob, c clob, a int array default array[]);");
    SQL.add("insert into blob_table (b, c, a) values (load_file('pom.xml'), convert('Hello', clob),  ARRAY[1,2,3])");
    SQL.add("create table big_table(id int primary key, name varchar(255))");
    for (int i = 0; i < 200; i++) {
      SQL.add("insert into big_table values(" + i + ", 'Hello')");
    }
  }

  @BeforeClass
  public static void resetDb() throws Exception {
    Connection conn = DriverManager.getConnection(options.getJdbcUrl());
    for (String sql : SQL) {
      conn.createStatement().execute(sql);
    }
  }

  @Override
  protected JDBCConnectOptions connectOptions() {
    return options;
  }

  @Test
  public void testSelect(TestContext should) {
    final Async test = should.async();

    String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";

    client
      .query(sql)
      .execute()
      .onFailure(should::fail)
      .onSuccess(rows -> {
        should.assertNotNull(rows);
        should.assertEquals(2, rows.size());
        should.assertTrue(rows.columnsNames().containsAll(Arrays.asList("ID", "FNAME", "LNAME")));
        int cnt = 0;
        for (Row row : rows) {
          cnt++;
          switch (cnt) {
            case 1:
              // by pos
              should.assertEquals(1, row.getInteger(0));
              should.assertEquals("john", row.getString(1));
              should.assertEquals("doe", row.getString(2));
              // by name
              should.assertEquals(1, row.getInteger("ID"));
              should.assertEquals("john", row.getString("FNAME"));
              should.assertEquals("doe", row.getString("LNAME"));
              break;
            case 2:
              // by pos
              should.assertEquals(2, row.getInteger(0));
              should.assertEquals("jane", row.getString(1));
              should.assertEquals("doe", row.getString(2));
              // by name
              should.assertEquals(2, row.getInteger("ID"));
              should.assertEquals("jane", row.getString("FNAME"));
              should.assertEquals("doe", row.getString("LNAME"));
              break;
          }
        }
        should.assertEquals(2, cnt);
        test.complete();
      });
  }

  @Test
  public void testInsert(TestContext should) {
    final Async test = should.async();

    String sql = "INSERT INTO insert_table (FNAME, LNAME) VALUES (?,?)";

    client
      .preparedQuery(sql)
      .execute(Tuple.of("Paulo", "Lopes"))
      .onFailure(should::fail)
      .onSuccess(rows -> {
        should.assertEquals(-1, rows.size());
        // updated rows
        should.assertEquals(1, rows.rowCount());

        Row lastInsertId = rows.property(JDBCPool.GENERATED_KEYS);
        should.assertNotNull(lastInsertId);
        should.assertTrue(lastInsertId.getLong(0) > 0);
        test.complete();
      });
  }

  @Test
  public void testSelectWithParams(TestContext should) {
    final Async test = should.async();

    String sql = "SELECT ID, FNAME, LNAME FROM select_table WHERE FNAME = ?";

    client
      .preparedQuery(sql)
      .execute(Tuple.of("john"))
      .onFailure(should::fail)
      .onSuccess(rows -> {
        should.assertNotNull(rows);
        should.assertEquals(1, rows.size());
        should.assertTrue(rows.columnsNames().containsAll(Arrays.asList("ID", "FNAME", "LNAME")));
        for (Row row : rows) {
          // by pos
          should.assertEquals(1, row.getInteger(0));
          should.assertEquals("john", row.getString(1));
          should.assertEquals("doe", row.getString(2));
          // by name
          should.assertEquals(1, row.getInteger("ID"));
          should.assertEquals("john", row.getString("FNAME"));
          should.assertEquals("doe", row.getString("LNAME"));
        }
        test.complete();
      });
  }

  @Test
  public void testSelectWithLabels(TestContext should) {
    final Async test = should.async();

    String sql = "SELECT ID as \"IdLabel\", FNAME as \"first_name\", LNAME as \"LAST.NAME\" FROM select_table WHERE fname = ?";

    client
      .preparedQuery(sql)
      .execute(Tuple.of("john"))
      .onFailure(should::fail)
      .onSuccess(rows -> {
        should.assertNotNull(rows);
        should.assertEquals(1, rows.size());
        should.assertTrue(rows.columnsNames().containsAll(Arrays.asList("IdLabel", "first_name", "LAST.NAME")));
        for (Row row : rows) {
          // by pos
          should.assertEquals(1, row.getInteger(0));
          should.assertEquals("john", row.getString(1));
          should.assertEquals("doe", row.getString(2));
          // by name
          should.assertEquals(1, row.getInteger("IdLabel"));
          should.assertEquals("john", row.getString("first_name"));
          should.assertEquals("doe", row.getString("LAST.NAME"));
        }
        test.complete();
      });
  }

  @Test
  public void testSelectWithTx(TestContext should) {
    final Async test = should.async();

    String sql = "INSERT INTO insert_table VALUES (?, ?, ?, ?);";

    client
      .getConnection()
      .onFailure(should::fail)
      .onSuccess(conn -> {
        conn.begin()
          .onFailure(should::fail)
          .onSuccess(tx -> {
            conn.preparedQuery(sql)
              .execute(Tuple.of(null, "smith", "john", "2003-03-03"))
              .onFailure(should::fail)
              .onSuccess(rows -> {
                should.assertNotNull(rows);
                should.assertEquals(-1, rows.size());
                // updated rows
                should.assertEquals(1, rows.rowCount());
                // extract the last inserted id
                int id = rows.property(JDBCPool.GENERATED_KEYS).getInteger(0);

                // verify that the data in stored
                conn
                  .preparedQuery("SELECT LNAME FROM insert_table WHERE id = ?")
                  .execute(Tuple.of(id))
                  .onFailure(should::fail)
                  .onSuccess(rows1 -> {
                    should.assertEquals(1, rows1.size());
                    should.assertEquals("smith", rows1.iterator().next().getString("LNAME"));

                    // rollback
                    tx.rollback()
                      .onFailure(should::fail)
                      .onSuccess(v -> {
                        conn.close()
                          .onFailure(should::fail)
                          .onSuccess(v1 -> {
                            // connection is returned to the pool
                            client
                              .preparedQuery("SELECT LNAME FROM insert_table WHERE id = ?")
                              .execute(Tuple.of(id))
                              .onFailure(should::fail)
                              .onSuccess(rows2 -> {
                                should.assertEquals(0, rows2.size());
                                test.complete();
                              });
                          });
                      });
                  });
              });
          });
      });
  }

  @Test
  public void testSelectInvalid(TestContext should) {
    final Async test = should.async();

    String sql = "SELECT FROM WHERE FOO BAR";

    client
      .query(sql)
      .execute()
      .onFailure(err -> test.complete())
      .onSuccess(rows -> should.fail("Broken SQL should fail"));
  }

  @Test
  public void testSelectBlob(TestContext should) {
    final Async test = should.async();

    String sql = "SELECT b FROM blob_table";

    client
      .query(sql)
      .execute()
      .onFailure(should::fail)
      .onSuccess(rows -> {
        should.assertEquals(1, rows.size());
        for (Row row : rows) {
          //should.assertNull(row.getString(0));
          should.assertNotNull(row.getBuffer(0));
        }
        test.complete();

      });
  }

  @Test
  public void testSelectClob(TestContext should) {
    final Async test = should.async();

    String sql = "SELECT c FROM blob_table";

    client
      .query(sql)
      .execute()
      .onFailure(should::fail)
      .onSuccess(rows -> {
        should.assertEquals(1, rows.size());
        for (Row row : rows) {
          should.assertNotNull(row.getString(0));
//          should.assertNull(row.getBuffer(0));
        }
        test.complete();

      });
  }

  @Test
  public void testSelectArray(TestContext should) {
    final Async test = should.async();

    String sql = "SELECT a FROM blob_table";

    client
      .query(sql)
      .execute()
      .onFailure(should::fail)
      .onSuccess(rows -> {
        should.assertEquals(1, rows.size());
        for (Row row : rows) {
          should.assertNotNull(row.getArrayOfIntegers(0));
          should.assertEquals(1, row.getArrayOfIntegers(0)[0]);
          should.assertEquals(2, row.getArrayOfIntegers(0)[1]);
          should.assertEquals(3, row.getArrayOfIntegers(0)[2]);
        }
        test.complete();

      });
  }

  @Test
  public void testBatchPreparedStatement(TestContext ctx) {
    client.query("drop table if exists t").execute(ctx.asyncAssertSuccess(res1 -> {
      client.query("create table t (u BIGINT)").execute(ctx.asyncAssertSuccess(res2 -> {
        List<Tuple> batch = Arrays.asList(
          Tuple.of(System.currentTimeMillis()),
          Tuple.of(System.currentTimeMillis()),
          Tuple.of(System.currentTimeMillis())
        );
        client.preparedQuery("insert into t (u) values (?)").executeBatch(batch, ctx.asyncAssertSuccess(res -> {
          ctx.assertEquals(3, res.rowCount());
        }));
      }));
    }));
  }

  @Test
  public void testPreparedStatementWithBufferParam(TestContext should) {
    Buffer buffer = Buffer.buffer("Hello world!");

    client
      .query("drop table if exists t")
      .execute()
      .compose(res1 -> client
        .query("create table t (b BLOB)")
        .execute()
        .compose(res2 -> client
          .preparedQuery("insert into t (b) values (?)")
          .execute(Tuple.of(buffer))
          .compose(res3 -> client
            .query("select b from t")
            .execute()
            .onComplete(should.asyncAssertSuccess(rows -> {
              should.assertEquals(1, rows.size());
              rows.forEach(row -> {
                Buffer actual = row.getBuffer(0);
                should.assertNotNull(actual);
                should.assertEquals(buffer, actual);
              });
            }))
          )
        )
      );
  }

  /**
   * This test checks if a connections is returned to the connection pool when the according queries is failing.
   *
   * <p>The superclass {@link io.vertx.jdbcclient.ClientTestBase} configures a pool with max size 1 (see {@link io.vertx.jdbcclient.ClientTestBase#poolOptions()}).
   * In this test two failing queries created by {@link io.vertx.sqlclient.Pool#query(String)} are executed.
   * The first one will fail with a {@link java.sql.SQLSyntaxErrorException}.
   * If the according connection is returned to the pool, the second query will fail immediately with the same {@link java.sql.SQLSyntaxErrorException}.
   * If the according connection is not closed, the pool will be exhausted and an according {@link java.sql.SQLException} is thrown after acquisition timeout.</p>
   */
  @Test
  public void testConnectionReturnedToPoolOnFailingQueryExecution(TestContext should) {
    final Async test = should.async();

    String sql = "SELECT FROM WHERE FOO BAR";

    client
      .query(sql)
      .execute()
      .onFailure(err -> {
        should.assertTrue(err instanceof SQLSyntaxErrorException, "Broken SQL should fail with SQLSyntaxErrorException");
        client
          .query(sql)
          .execute()
          .onFailure(err2 -> {
            should.assertTrue(err2 instanceof SQLSyntaxErrorException, "Broken SQL should fail with SQLSyntaxErrorException");
            test.complete();
          })
          .onSuccess(rows -> should.fail("Broken SQL should fail"));
      })
      .onSuccess(rows -> should.fail("Broken SQL should fail"));
  }

  /**
   * Same as {@link io.vertx.jdbcclient.JDBCPoolTest#testConnectionReturnedToPoolOnFailingQueryExecution(TestContext)} but uses queries created by
   * {@link io.vertx.sqlclient.Pool#withConnection(Function)} together with {@link io.vertx.sqlclient.SqlClient#query(String)}.
   */
  @Test
  public void testConnectionReturnedToPoolOnFailingQueryExecutionWhenUsingWithConnection(TestContext should) {
    final Async test = should.async();

    String sql = "SELECT FROM WHERE FOO BAR";

    client
      .withConnection(conn -> conn
        .query(sql)
        .execute()
      )
      .onSuccess(rows -> should.fail("Broken SQL should fail"))
      .onFailure(err -> {
        should.assertTrue(err instanceof SQLSyntaxErrorException, "Broken SQL should fail with SQLSyntaxErrorException");
        client
          .withConnection(conn2 -> conn2
            .query(sql)
            .execute()
            .onFailure(err2 -> {
              should.assertTrue(err2 instanceof SQLSyntaxErrorException, "Broken SQL should fail with SQLSyntaxErrorException");
              test.complete();
            })
            .onSuccess(rows -> should.fail("Broken SQL should fail"))
          );
      });
  }

  @Test
  public void testUnwrapToJDBCConnection(TestContext should) {
    client
      .getConnection()
      .onComplete(should.asyncAssertSuccess(conn -> {
      Connection c = JDBCUtils.unwrap(conn);
      assertNotNull(c);
    }));
  }
}
