package io.vertx.jdbcclient;

import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.*;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(VertxUnitRunner.class)
public class ClientTest extends ClientTestBase {

  protected SqlConnection connection() throws Exception {
    return client.getConnection().toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);
  }

  @Test
  public void testConnectionSelect(TestContext ctx) throws Exception {
    testSelect(ctx, connection());
  }

  @Test
  public void testClientSelect(TestContext ctx) throws Exception {
    testSelect(ctx, client);
  }

  private void testSelect(TestContext ctx, SqlClient client) throws Exception {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
    client
      .query(sql)
      .execute()
      .onComplete(ctx.asyncAssertSuccess(resultSet -> {
        ctx.assertEquals(2, resultSet.size());
        ctx.assertEquals("ID", resultSet.columnsNames().get(0));
        ctx.assertEquals("FNAME", resultSet.columnsNames().get(1));
        ctx.assertEquals("LNAME", resultSet.columnsNames().get(2));
        RowIterator<Row> it = resultSet.iterator();
        Row result0 = it.next();
        ctx.assertEquals(1, (int) result0.getInteger(0));
        ctx.assertEquals("john", result0.getString(1));
        ctx.assertEquals("doe", result0.getString(2));
        Row result1 = it.next();
        ctx.assertEquals(2, (int) result1.getInteger(0));
        ctx.assertEquals("jane", result1.getString(1));
        ctx.assertEquals("doe", result1.getString(2));
      }));
  }

  @Test
  public void testInsertWithParameters(TestContext ctx) throws Exception {
    // final TimeZone tz = TimeZone.getDefault();
    // TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    SqlClient conn = connection();
    String sql = "INSERT INTO insert_table VALUES (?, ?, ?, ?);";
    LocalDate expected = LocalDate.of(2002, 2, 2);
    conn
      .preparedQuery(sql).execute(Tuple.of(0, "doe", "jane", expected))
      .onComplete(ctx.asyncAssertSuccess(rowSet -> {
      // assertUpdate(result, 1);
      conn
        .preparedQuery("SElECT DOB FROM insert_table WHERE id=?")
        .execute(Tuple.of(0))
        .onComplete(ctx.asyncAssertSuccess(rs -> {
        ctx.assertEquals(1, rs.size());
        ctx.assertEquals(expected, rs.iterator().next().getLocalDate(0));
      }));
    }));
  }

  @Test
  public void testTransactionCommit(TestContext ctx) throws Exception {
    testTransaction(ctx, true);
  }

  @Test
  public void testTransactionRollback(TestContext ctx) throws Exception {
    testTransaction(ctx, false);
  }

  private void testTransaction(TestContext testCtx, boolean commit) throws Exception {
    SqlConnection conn = connection();
    conn
      .begin()
      .onComplete(testCtx.asyncAssertSuccess(tx -> {
      String sql = "INSERT INTO insert_table VALUES (?, ?, ?, ?);";
      LocalDate expected = LocalDate.of(2002, 2, 2);
      conn
        .preparedQuery(sql)
        .execute(Tuple.of(0, "doe", "jane", expected))
        .onComplete(testCtx.asyncAssertSuccess(rowSet -> {
          if (commit) {
            tx.commit()
              .onComplete(testCtx.asyncAssertSuccess(v2 -> {
              conn
                .preparedQuery("SElECT DOB FROM insert_table WHERE id=?")
                .execute(Tuple.of(0))
                .onComplete(testCtx.asyncAssertSuccess(rs -> {
                  testCtx.assertEquals(1, rs.size());
                  testCtx.assertEquals(expected, rs.iterator().next().getLocalDate(0));
                }));
            }));
          } else {
            tx.rollback()
              .onComplete(testCtx.asyncAssertSuccess(v2 -> {
              conn
                .preparedQuery("SElECT DOB FROM insert_table WHERE id=?")
                .execute(Tuple.of(0))
                .onComplete(testCtx.asyncAssertSuccess(rs -> {
                  testCtx.assertEquals(0, rs.size());
                }));
            }));
          }
        }));
    }));
  }

  @Test
  public void testStream(TestContext should) {
    client.<List<Row>>withTransaction(tx ->
      tx.prepare("SELECT CURRENT_DATE AS today, CURRENT_TIME AS now FROM (VALUES(0))")
        .map(pS -> pS.createStream(200))
        .flatMap(stream -> Future
          .future(promise -> {
            List<Row> rows = new ArrayList<>();
            stream.exceptionHandler(promise::fail);
            stream.endHandler(v -> promise.complete(rows));
            stream.handler(row -> {
              should.assertEquals(0, rows.size());
              rows.add(row);
            });
          })
        )
    )
      .onComplete(should.asyncAssertSuccess(rows -> {
        should.assertEquals(1, rows.size());
      }));
  }
}
