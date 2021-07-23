package io.vertx.jdbcclient;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.h2.api.TimestampWithTimeZone;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public class JDBCPoolDateTimeTest extends ClientTestBase {

  private static final JDBCConnectOptions options = new JDBCConnectOptions()
    .setJdbcUrl("jdbc:h2:mem:test-" + JDBCPoolDateTimeTest.class.getSimpleName() + ";DB_CLOSE_DELAY=-1");

  private static final List<String> SQL = new ArrayList<>();

  static {
    SQL.add("drop table if exists t");
    SQL.add("create table t (tstz TIMESTAMP WITH TIME ZONE, ts TIMESTAMP)");
  }

  public static void resetDb() throws Exception {
    Connection conn = DriverManager.getConnection(options.getJdbcUrl());
    for (String sql : SQL) {
      conn.createStatement().execute(sql);
    }
  }

  @Before
  public void setUp() throws Exception {
    resetDb();
    super.setUp();
  }

  @Override
  protected JDBCConnectOptions connectOptions() {
    return options;
  }

  @Test
  public void testLocalDateTime(TestContext should) {
    final Async test = should.async();
    String sql = "INSERT INTO t (ts) VALUES (?)";
    LocalDateTime localDateTime = LocalDateTime.parse("2021-07-21T19:16:33");
    System.out.println(localDateTime);
    client
      .preparedQuery(sql)
      .execute(Tuple.of(localDateTime))
      .onFailure(should::fail)
      .onSuccess(rows -> client
        .query("SELECT ts FROM t")
        .execute()
        .onFailure(should::fail)
        .onSuccess(rows2 -> {
          Row row = rows2.iterator().next();
          should.assertEquals(localDateTime, row.getLocalDateTime("TS"));
          test.complete();
        }));
  }

  @Test
  public void testOffsetDateTime(TestContext should) {
    final Async flag = should.async();

    String sql = "INSERT INTO t (tstz) VALUES (?)";
    OffsetDateTime now = OffsetDateTime.parse("2021-07-21T19:16:33Z");
    client
      .preparedQuery(sql)
      .execute(Tuple.of(now))
      .onFailure(should::fail)
      .onSuccess(rows -> client
        .query("SELECT tstz from t")
        .execute()
        .onSuccess(rows2 -> {
          Row row = rows2.iterator().next();
          should.verify(i -> {
            final Object value = row.getValue("TSTZ");
            should.assertNotNull(value);
            should.assertEquals(TimestampWithTimeZone.class, value.getClass());
          });
          flag.complete();
        })
        .onFailure(should::fail));
  }
}
