/*
 * Copyright (c) 2011-2026 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *      The Eclipse Public License is available at
 *      http://www.eclipse.org/legal/epl-v10.html
 *
 *      The Apache License v2.0 is available at
 *      http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.it;

import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public abstract class SavepointTestBase {

  protected abstract Pool pool();

  protected abstract Pool checkerPool();

  protected boolean supportsSavepointRelease() {
    return true;
  }

  private Future<Void> cleanMutable() {
    return pool().query("DELETE FROM mutable").execute().mapEmpty();
  }

  private Future<List<Integer>> readMutableIds() {
    return checkerPool().query("SELECT id FROM mutable ORDER BY id").execute().map(rows -> {
      List<Integer> ids = new ArrayList<>();
      for (Row row : rows) {
        ids.add(row.getInteger(0));
      }
      return ids;
    });
  }

  @Test
  public void testRollbackToSavepointUndoesWorkAfterIt(TestContext should) {
    cleanMutable().onComplete(should.asyncAssertSuccess(v0 -> {
      pool().getConnection().onComplete(should.asyncAssertSuccess(conn -> {
        conn.begin().onComplete(should.asyncAssertSuccess(tx -> {
          conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
            .execute(Tuple.of(1, "a"))
            .compose(v -> tx.createSavepoint())
            .compose(sp -> {
              return conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
                .execute(Tuple.of(2, "b"))
                .compose(v -> sp.rollback());
            })
            .compose(v -> conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
              .execute(Tuple.of(3, "c")))
            .compose(v -> tx.commit())
            .compose(v -> conn.close())
            .compose(v -> readMutableIds())
            .onComplete(should.asyncAssertSuccess(ids -> {
              should.assertEquals(List.of(1, 3), ids);
            }));
        }));
      }));
    }));
  }

  @Test
  public void testNestedSavepoints(TestContext should) {
    cleanMutable().onComplete(should.asyncAssertSuccess(v0 -> {
      pool().getConnection().onComplete(should.asyncAssertSuccess(conn -> {
        conn.begin().onComplete(should.asyncAssertSuccess(tx -> {
          conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
            .execute(Tuple.of(1, "a"))
            .compose(v -> tx.createSavepoint())
            .compose(outerSp -> {
              return conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
                .execute(Tuple.of(2, "b"))
                .compose(v -> tx.createSavepoint())
                .compose(innerSp -> {
                  return conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
                    .execute(Tuple.of(3, "c"))
                    .compose(v -> innerSp.rollback());
                })
                .compose(v -> outerSp.rollback());
            })
            .compose(v -> tx.commit())
            .compose(v -> conn.close())
            .compose(v -> readMutableIds())
            .onComplete(should.asyncAssertSuccess(ids -> {
              should.assertEquals(List.of(1), ids);
            }));
        }));
      }));
    }));
  }

  @Test
  public void testWholeTransactionRollbackDiscardsSavepointWork(TestContext should) {
    cleanMutable().onComplete(should.asyncAssertSuccess(v0 -> {
      pool().getConnection().onComplete(should.asyncAssertSuccess(conn -> {
        conn.begin().onComplete(should.asyncAssertSuccess(tx -> {
          conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
            .execute(Tuple.of(1, "a"))
            .compose(v -> tx.createSavepoint())
            .compose(sp -> {
              return conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
                .execute(Tuple.of(2, "b"));
            })
            .compose(v -> tx.rollback())
            .compose(v -> conn.close())
            .compose(v -> readMutableIds())
            .onComplete(should.asyncAssertSuccess(ids -> {
              should.assertTrue(ids.isEmpty());
            }));
        }));
      }));
    }));
  }

  @Test
  public void testReleaseSavepointKeepsWork(TestContext should) {
    if (!supportsSavepointRelease()) {
      return;
    }
    cleanMutable().onComplete(should.asyncAssertSuccess(v0 -> {
      pool().getConnection().onComplete(should.asyncAssertSuccess(conn -> {
        conn.begin().onComplete(should.asyncAssertSuccess(tx -> {
          conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
            .execute(Tuple.of(1, "a"))
            .compose(v -> tx.createSavepoint())
            .compose(sp -> {
              return conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
                .execute(Tuple.of(2, "b"))
                .compose(v -> sp.release());
            })
            .compose(v -> tx.commit())
            .compose(v -> conn.close())
            .compose(v -> readMutableIds())
            .onComplete(should.asyncAssertSuccess(ids -> {
              should.assertEquals(List.of(1, 2), ids);
            }));
        }));
      }));
    }));
  }

  @Test
  public void testNamedSavepoint(TestContext should) {
    cleanMutable().onComplete(should.asyncAssertSuccess(v0 -> {
      pool().getConnection().onComplete(should.asyncAssertSuccess(conn -> {
        conn.begin().onComplete(should.asyncAssertSuccess(tx -> {
          conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
            .execute(Tuple.of(1, "a"))
            .compose(v -> tx.createSavepoint("my_sp"))
            .compose(sp -> {
              return conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
                .execute(Tuple.of(2, "b"))
                .compose(v -> sp.rollback());
            })
            .compose(v -> tx.commit())
            .compose(v -> conn.close())
            .compose(v -> readMutableIds())
            .onComplete(should.asyncAssertSuccess(ids -> {
              should.assertEquals(List.of(1), ids);
            }));
        }));
      }));
    }));
  }

  @Test
  public void testCanCreateNewSavepointAfterRollback(TestContext should) {
    cleanMutable().onComplete(should.asyncAssertSuccess(v0 -> {
      pool().getConnection().onComplete(should.asyncAssertSuccess(conn -> {
        conn.begin().onComplete(should.asyncAssertSuccess(tx -> {
          conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
            .execute(Tuple.of(1, "a"))
            .compose(v -> tx.createSavepoint())
            .compose(sp1 -> {
              return conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
                .execute(Tuple.of(2, "b"))
                .compose(v -> sp1.rollback());
            })
            .compose(v -> tx.createSavepoint())
            .compose(sp2 -> {
              return conn.preparedQuery("INSERT INTO mutable (id, val) VALUES (?, ?)")
                .execute(Tuple.of(3, "c"))
                .compose(v -> sp2.rollback());
            })
            .compose(v -> tx.commit())
            .compose(v -> conn.close())
            .compose(v -> readMutableIds())
            .onComplete(should.asyncAssertSuccess(ids -> {
              should.assertEquals(List.of(1), ids);
            }));
        }));
      }));
    }));
  }
}
