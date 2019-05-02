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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.sync.ZKTreeLockManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRecursiveDelete extends HBaseObjectStoreSemanticsTest {
  public static final Logger LOG =
        LoggerFactory.getLogger(TestRecursiveDelete.class);

  @Test
  public void testCreationAfterDeletion() throws Exception {
    // This was an observed failure condition where creating a
    // directory that we had just removed would be deadlocked
    // because the thread that issued the original deletion still
    // held the lock on the directory it deleted.
    ExecutorService svc = Executors.newSingleThreadExecutor();
    Path tmp = testPath(".tmp");
    Path lock = testPath(".tmp/hbase-hbck.lock");

    try {
      hboss.mkdirs(tmp);
      hboss.delete(tmp, true);
      LOG.info("After mkdir and delete in test thread");
      summarizeZKLocks();

      Future<?> fut = svc.submit(new Runnable() {
        public void run() {
          try {
            hboss.mkdirs(tmp);
            LOG.info("After mkdir in separate thread");
            summarizeZKLocks();
            try (FSDataOutputStream out = hboss.create(lock)) {
              out.write(Bytes.toBytes("localhost"));
              out.flush();
            }
          } catch (Exception e) {
            LOG.error("Caught exception", e);
            Assert.fail("Failed to create file");
          }
        }
      });
      Assert.assertNull(fut.get(15, TimeUnit.SECONDS));
    } finally {
      hboss.delete(tmp, true);
    }
  }

  void summarizeZKLocks() {
    if (sync instanceof ZKTreeLockManager) {
      LOG.info(((ZKTreeLockManager) sync).summarizeLocks());
    }
  }
}
