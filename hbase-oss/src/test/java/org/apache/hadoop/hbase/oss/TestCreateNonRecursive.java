/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.oss;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestCreateNonRecursive extends HBaseObjectStoreSemanticsTest {

  @Test
  public void testCreateNonRecursiveSerial() throws Exception {
    Path serialPath = testPath("testCreateNonRecursiveSerial");
    try {
      FSDataOutputStream out;

      out = hboss.createNonRecursive(serialPath, false, 1024, (short)1, 1024, null);
      out.close();

      intercept(FileAlreadyExistsException.class, () ->
        hboss.createNonRecursive(serialPath, false, 1024, (short)1, 1024, null));
    } finally {
      hboss.delete(serialPath);
    }
  }

  @Test
  public void testCreateNonRecursiveParallel() throws Exception {
    int experiments = 10;
    int experimentSize = 10;
    for (int e = 0; e < experiments; e++) {
      ArrayList<Callable<Boolean>> callables = new ArrayList<>(experimentSize);
      ArrayList<Future<Boolean>> futures = new ArrayList<>(experimentSize);

      Path parallelPath = testPath("testCreateNonRecursiveParallel" + e);
      ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(10);
      executor.prestartAllCoreThreads();
      for (int i = 0; i < experimentSize; i++) {
        callables.add(() -> {
          FSDataOutputStream out = null;
          boolean exceptionThrown = false;
          try {
            out = hboss.createNonRecursive(parallelPath, false, 1024, (short)1, 1024, null);
          } catch(FileAlreadyExistsException e1) {
            exceptionThrown = true;
          } finally {
            if (out != null) {
              out.close();
            }
          }
          return exceptionThrown;
        });
      }
      try {
        for (Callable<Boolean> callable : callables) {
          // This is in a separate loop to try and get them all as overlapped as possible
          futures.add(executor.submit(callable));
        }
        int exceptionsThrown = 0;
        for (Future<Boolean> future : futures) {
          // This is in a separate loop to try and get them all as overlapped as possible
          if (future.get()) {
            exceptionsThrown++;
          }
        }
        Assert.assertEquals("All but exactly 1 call should have thrown exceptions. " +
              "Experiment " + (e+1) + " of " + experiments + ".",
              experimentSize - 1, exceptionsThrown);
      } finally {
        hboss.delete(parallelPath);
        executor.shutdown();
      }
    }
  }
}
