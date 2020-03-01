package io.vertx.jdbcclient;

import io.vertx.core.Context;
import io.vertx.ext.jdbc.JDBCClientTestBase;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class ClientTest extends JDBCClientTestBase {

  private JDBCPool client;

  @Before
  public void setUp() throws Exception {
    resetDb();
    super.setUp();
    client = JDBCPool.create(vertx, config());
  }

  @After
  public void after() throws Exception {
    client.close();
    super.after();
  }

  protected SqlConnection connection() {
    return connection(vertx.getOrCreateContext());
  }

  protected SqlConnection connection(Context context) {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<SqlConnection> ref = new AtomicReference<>();
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

  @Test
  public void testConnectionSelect() {
    testSelect(connection());
  }

  @Test
  public void testClientSelect() {
    testSelect(client);
  }

  private void testSelect(SqlClient client) {
    String sql = "SELECT ID, FNAME, LNAME FROM select_table ORDER BY ID";
    client.query(sql, onSuccess(resultSet -> {
      assertEquals(2, resultSet.size());
      assertEquals("ID", resultSet.columnsNames().get(0));
      assertEquals("FNAME", resultSet.columnsNames().get(1));
      assertEquals("LNAME", resultSet.columnsNames().get(2));
      RowIterator<Row> it = resultSet.iterator();
      Row result0 = it.next();
      assertEquals(1, (int) result0.getInteger(0));
      assertEquals("john", result0.getString(1));
      assertEquals("doe", result0.getString(2));
      Row result1 = it.next();
      assertEquals(2, (int) result1.getInteger(0));
      assertEquals("jane", result1.getString(1));
      assertEquals("doe", result1.getString(2));
      testComplete();
    }));
    await();
  }

  @Test
  public void testInsertWithParameters() {
    // final TimeZone tz = TimeZone.getDefault();
    // TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    SqlClient conn = connection();
    String sql = "INSERT INTO insert_table VALUES (?, ?, ?, ?);";
    LocalDate expected = LocalDate.of(2002, 2, 2);
    conn.preparedQuery(sql, Tuple.of(0, "doe", "jane", expected), onSuccess(rowSet -> {
      // assertUpdate(result, 1);
      conn.preparedQuery("SElECT DOB FROM insert_table WHERE id=?", Tuple.of(0), onSuccess(rs -> {
        assertEquals(1, rs.size());
        assertEquals(expected, rs.iterator().next().getLocalDate(0));
        testComplete();
      }));
    }));
    await();
  }

  @Test
  public void testTransactionCommit() {
    testTransaction(true);
  }

  @Test
  public void testTransactionRollback() {
    testTransaction(false);
  }

  private void testTransaction(boolean commit) {
    Context ctx = vertx.getOrCreateContext();
    SqlConnection conn = connection(ctx);
    ctx.runOnContext(v -> {
      Transaction tx = conn.begin();
      String sql = "INSERT INTO insert_table VALUES (?, ?, ?, ?);";
      LocalDate expected = LocalDate.of(2002, 2, 2);
      conn.preparedQuery(sql, Tuple.of(0, "doe", "jane", expected), onSuccess(rowSet -> {
        if (commit) {
          tx.commit(onSuccess(v2 -> {
            conn.preparedQuery("SElECT DOB FROM insert_table WHERE id=?", Tuple.of(0), onSuccess(rs -> {
              assertEquals(1, rs.size());
              assertEquals(expected, rs.iterator().next().getLocalDate(0));
              testComplete();
            }));
          }));
        } else {
          tx.rollback(onSuccess(v2 -> {
            conn.preparedQuery("SElECT DOB FROM insert_table WHERE id=?", Tuple.of(0), onSuccess(rs -> {
              assertEquals(0, rs.size());
              testComplete();
            }));
          }));
        }
      }));
    });
    await();
  }
}
