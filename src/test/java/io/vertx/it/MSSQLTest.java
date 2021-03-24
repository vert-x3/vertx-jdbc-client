package io.vertx.it;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class MSSQLTest extends MSSQLTestBase {

  public MSSQLTest() {
    poolSize = 1;
    isJdbcPool = true;
  }

  @Test
  public void simpleTest(TestContext should) {
    final Async test = should.async();
    // this test would fail if we would attempt to read the generated ids after the end of the cursor
    // the fix implies that we must read them before we close the cursor.
    client
      .preparedQuery("select * from Fortune")
      .execute(should.asyncAssertSuccess(resultSet -> {
        should.assertEquals(12, resultSet.size());
        test.complete();
      }));
  }

}
