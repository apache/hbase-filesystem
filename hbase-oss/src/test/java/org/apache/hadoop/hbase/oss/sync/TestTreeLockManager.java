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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.oss.sync.AutoLock;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager.Depth;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTreeLockManager extends HBaseObjectStoreSemanticsTest {
  public static final Logger LOG =
        LoggerFactory.getLogger(TestTreeLockManager.class);

  @Test
  public void testLockBelowChecks() throws Exception {
    Path parent = testPath("testListingLevels");
    Path child = new Path(parent, "child");
    Path deepWrite = new Path(child, "w");
    Path deepRead = new Path(child, "r");

    AutoLock readLock = sync.lock(deepRead);
    AutoLock writeLock = sync.lockWrite(deepWrite);

    final int FALSE = 0;
    final int TRUE = 1;
    final int UNSET = 2;

    AtomicInteger dirDepthDeeperReadLock = new AtomicInteger(UNSET);
    AtomicInteger recDepthDeeperReadLock = new AtomicInteger(UNSET);
    AtomicInteger dirDepthDeeperWriteLock = new AtomicInteger(UNSET);
    AtomicInteger recDepthDeeperWriteLock = new AtomicInteger(UNSET);
    AtomicInteger dirDepthShallowReadLock = new AtomicInteger(UNSET);
    AtomicInteger recDepthShallowReadLock = new AtomicInteger(UNSET);
    AtomicInteger dirDepthShallowWriteLock = new AtomicInteger(UNSET);
    AtomicInteger recDepthShallowWriteLock = new AtomicInteger(UNSET);
    AtomicBoolean threadCompletedSuccessfully = new AtomicBoolean(false);

    try {
      Runnable r = new Runnable() {
        public void run() {
          try {
            dirDepthDeeperReadLock.set(
                  sync.readLockBelow(sync.norm(parent), Depth.DIRECTORY) ?
                  TRUE : FALSE);
            recDepthDeeperReadLock.set(
                  sync.readLockBelow(sync.norm(parent), Depth.RECURSIVE) ?
                  TRUE : FALSE);
            dirDepthDeeperWriteLock.set(
                  sync.writeLockBelow(sync.norm(parent), Depth.DIRECTORY) ?
                  TRUE : FALSE);
            recDepthDeeperWriteLock.set(
                  sync.writeLockBelow(sync.norm(parent), Depth.RECURSIVE) ?
                  TRUE : FALSE);
            dirDepthShallowReadLock.set(
                  sync.readLockBelow(sync.norm(child), Depth.DIRECTORY) ?
                  TRUE : FALSE);
            recDepthShallowReadLock.set(
                  sync.readLockBelow(sync.norm(child), Depth.RECURSIVE) ?
                  TRUE : FALSE);
            dirDepthShallowWriteLock.set(
                  sync.writeLockBelow(sync.norm(child), Depth.DIRECTORY) ?
                  TRUE : FALSE);
            recDepthShallowWriteLock.set(
                  sync.writeLockBelow(sync.norm(child), Depth.RECURSIVE) ?
                  TRUE : FALSE);
            threadCompletedSuccessfully.set(true);
          } catch (IOException e) {
            LOG.error("Exception in side-thread: {}", e);
            e.printStackTrace();
          }
        }
      };
      Thread t = new Thread(r);
      t.start();
      t.join();

      // Asserts have to be checked from a different thread than the locks were
      // acquired in, because the locks are reentrant.
      Assert.assertTrue("Test thread did not complete successfully; see logs " +
          "for exceptions.", threadCompletedSuccessfully.get());
      Assert.assertEquals(
          "Directory-depth check found read-lock 2 levels down; shouldn't have",
          FALSE, dirDepthDeeperReadLock.get());
      Assert.assertEquals(
          "Recursive check failed to find found write-lock 2 levels down",
          TRUE, recDepthDeeperReadLock.get());
      Assert.assertEquals(
          "Directory-depth check found write-lock 2 levels down; shouldn't have",
          FALSE, dirDepthDeeperWriteLock.get());
      Assert.assertEquals(
          "Recursive check failed to find found write-lock 2 levels down",
          TRUE, recDepthDeeperWriteLock.get());
      Assert.assertEquals(
          "Directory-depth check failed to find found read-lock 1 level down",
          TRUE, dirDepthShallowReadLock.get());
      Assert.assertEquals(
          "Directory-depth check failed to find found read-lock 1 level down",
          TRUE, recDepthShallowReadLock.get());
      Assert.assertEquals(
          "Recursive check failed to find found write-lock 1 level down",
          TRUE, dirDepthShallowWriteLock.get());
      Assert.assertEquals(
          "Recursive check failed to find found write-lock 1 level down",
          TRUE, recDepthShallowWriteLock.get());
    } finally {
      readLock.close();
      writeLock.close();
    }
  }
}
