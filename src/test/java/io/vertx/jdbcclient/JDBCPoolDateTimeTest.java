package io.vertx.jdbcclient;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

@RunWith(VertxUnitRunner.class)
public class JDBCPoolDateTimeTest extends ClientTestBase {

  private static final JDBCConnectOptions options = new JDBCConnectOptions()
    .setJdbcUrl("jdbc:h2:mem:test-" + JDBCPoolDateTimeTest.class.getSimpleName() + ";DB_CLOSE_DELAY=-1");

  private static final List<String> SQL = new ArrayList<>();

  static {
    SQL.add("drop table if exists tt");
    SQL.add("create table tt (tstz TIMESTAMP WITH TIME ZONE, ts TIMESTAMP, t TIME, ttz TIME WITH TIME ZONE )");
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
    String sql = "INSERT INTO tt (ts) VALUES (?)";
    LocalDateTime localDateTime = LocalDateTime.parse("2021-07-21T19:16:33");
    System.out.println(localDateTime);
    client
      .preparedQuery(sql)
      .execute(Tuple.of(localDateTime))
      .onFailure(should::fail)
      .onSuccess(rows -> client
        .query("SELECT ts FROM tt")
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

    String sql = "INSERT INTO tt (tstz) VALUES (?)";
    OffsetDateTime odt = OffsetDateTime.parse("2021-07-21T19:16:33+03:00");
    System.out.println(odt);
    client
      .preparedQuery(sql)
      .execute(Tuple.of(odt))
      .onFailure(should::fail)
      .onSuccess(rows -> client
        .query("SELECT tstz from tt")
        .execute()
        .onSuccess(rows2 -> {
          Row row = rows2.iterator().next();
          should.verify(i -> {
            final Object value = row.getValue("TSTZ");
            should.assertNotNull(value);
            should.assertEquals(OffsetDateTime.class, value.getClass());
            should.assertEquals(odt, value);
          });
          flag.complete();
        })
        .onFailure(should::fail));
  }

  @Test
  public void testLocalTime(TestContext should) {
    final Async flag = should.async();

    String sql = "INSERT INTO tt (t) VALUES (?)";
    LocalTime localTime = LocalTime.parse("19:16:33");
    System.out.println(localTime);
    client
      .preparedQuery(sql)
      .execute(Tuple.of(localTime))
      .onFailure(should::fail)
      .onSuccess(rows -> client
        .query("SELECT t from tt")
        .execute()
        .onSuccess(rows2 -> {
          Row row = rows2.iterator().next();
          should.verify(i -> {
            final Object value = row.getValue("T");
            should.assertNotNull(value);
            should.assertEquals(LocalTime.class, value.getClass());
            should.assertEquals(localTime, value);
          });
          flag.complete();
        })
        .onFailure(should::fail));
  }

  @Test
  public void testOffsetTime(TestContext should) {
    final Async flag = should.async();

    String sql = "INSERT INTO tt (ttz) VALUES (?)";
    OffsetTime offsetTime = OffsetTime.of(LocalTime.of(7, 0), ZoneOffset.ofHours(7));
    System.out.println(offsetTime);
    client
      .preparedQuery(sql)
      .execute(Tuple.of(offsetTime))
      .onFailure(should::fail)
      .onSuccess(rows -> client
        .query("SELECT ttz from tt")
        .execute()
        .onSuccess(rows2 -> {
          Row row = rows2.iterator().next();
          should.verify(i -> {
            final Object value = row.getValue("TTZ");
            should.assertNotNull(value);
            should.assertEquals(OffsetTime.class, value.getClass());
            should.assertEquals(offsetTime, value);
          });
          flag.complete();
        })
        .onFailure(should::fail));
  }
}
