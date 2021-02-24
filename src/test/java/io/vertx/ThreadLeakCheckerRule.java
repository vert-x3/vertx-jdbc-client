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
package io.vertx;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

public class ThreadLeakCheckerRule implements TestRule {

  private final Predicate<Thread> predicate;

  public ThreadLeakCheckerRule() {
    this(t ->
      t.getName().equals("vertx-jdbc-service-get-connection-thread") ||
      t.getName().startsWith("C3P0PooledConnectionPoolManager")
    );
  }

  public ThreadLeakCheckerRule(Predicate<Thread> predicate) {
    this.predicate = predicate;
  }

  @Override
  public Statement apply(Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        check("before");
        statement.evaluate();
        check("after");
      }
    };
  }

  private void check(String when) {
    // Make a check
    List<Thread> threads = findThreads(predicate);
    if (threads.size() > 0) {
      StringBuilder msg = new StringBuilder(threads
        .stream()
        .map(t -> t.getName() + ": state=" + t.getState().name() + "/alive=" + t.isAlive())
        .collect(Collectors.joining(", ", "Unexpected threads " + when + " test:", ".")));
      ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
      for (ThreadInfo ti : threadMxBean.dumpAllThreads(true, true)) {
        msg.append(System.getProperty("line.separator")).append(ti.toString());
      }
      fail(msg.toString());
    }
  }

  public static List<Thread> findThreads(Predicate<Thread> predicate) {
    return Thread.getAllStackTraces()
      .keySet()
      .stream()
      .filter(predicate)
      .collect(Collectors.toList());
  }
}
