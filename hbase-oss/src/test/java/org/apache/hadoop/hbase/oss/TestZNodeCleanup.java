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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.sync.ZKTreeLockManager;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates that when some entry in the object store is deleted, the corresponding
 * data in ZooKeeper is also deleted.
 */
public class TestZNodeCleanup extends HBaseObjectStoreSemanticsTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestZNodeCleanup.class);

  private ZKTreeLockManager lockManager;
  private ZooKeeper zk;

  @Before
  public void setup() throws Exception {
    super.setup();
    assumeTrue("Lock manager is a " + getLockManager().getClass(),
        getLockManager() instanceof ZKTreeLockManager);
    lockManager = (ZKTreeLockManager) getLockManager();
    Configuration conf = hboss.getConf();
    LOG.info("Waiting for ZK client to connect");
    // TODO should wait for ZK to connect
    final CountDownLatch latch = new CountDownLatch(1);
    // Root the ZK connection beneath /hboss
    zk = new ZooKeeper(conf.get(Constants.ZK_CONN_STRING) + "/hboss", 60000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        LOG.info("Caught event " + event);
        if (event.getState() == KeeperState.SyncConnected) {
          latch.countDown();
        }
      }
    });
    latch.await();
    LOG.info("ZooKeeper client is connected");
  }

  @After
  public void teardown() throws Exception {
    if (lockManager != null) {
      String zkRoot = lockManager.norm(TestUtils.testPathRoot(hboss)).toString();
      LOG.info("Dumping contents of ZooKeeper after test from {}", zkRoot);
      printZkBFS(zkRoot);
    }
    if (zk != null) {
      zk.close();
      zk = null;
    }
  }

  void printZkBFS(String path) throws Exception {
    LOG.info(path);
    List<String> children = zk.getChildren(path, false);
    for (String child : children) {
      printZkBFS(path + "/" + child);
    }
  }

  String getZNodeFromPath(Path p) {
    return Path.getPathWithoutSchemeAndAuthority(p).toString().substring(1);
  }

  void validatePathInZk(Path zkPath) throws Exception {
    assertNotNull(zkPath + " did not exist in ZK", zk.exists(zkPath.toString(), false));
    String hbossLock = new Path(zkPath, ZKTreeLockManager.LOCK_SUB_ZNODE).toString();
    assertNotNull(hbossLock + " did not exist in ZK", zk.exists(hbossLock, false));
  }

  void validatePathNotInZk(Path zkPath) throws Exception {
    assertNull(zkPath + " incorrectly exists in ZK.", zk.exists(zkPath.toString(), false));
  }

  @Test
  public void testRename() throws Exception {
    // Rename src to dest and validate that the znode for src is cleaned up.
    final Path src = TestUtils.testPath(hboss, "src");
    final Path dest = TestUtils.testPath(hboss, "dest");
    assertTrue(hboss.mkdirs(src));
    // The src znode should exist after creating the dir in S3
    validatePathInZk(lockManager.norm(src));
    // `mv src dest`
    assertTrue(hboss.rename(src, dest));
    // We should have a znode for dest (we just locked it)
    validatePathInZk(lockManager.norm(dest));
    // We should no longer have a znode for src (we effectively deleted it from S3)
    validatePathNotInZk(lockManager.norm(src));
  }

  @Test
  public void testFailedRename() throws Exception {
    // Rename src to dest and validate that the znode for src is cleaned up.
    final Path src = TestUtils.testPathRoot(hboss);
    final Path dest = TestUtils.testPath(hboss, "dest");
    assertTrue(hboss.mkdirs(src));
    // The src znode should exist after creating the dir in S3
    validatePathInZk(lockManager.norm(src));
    // The move should fail
    assertFalse(hboss.rename(src, dest));
    // We should have a znode for dest (we just locked it)
    validatePathInZk(lockManager.norm(src));
    // We should no longer have a znode for src (we effectively deleted it from S3)
    validatePathInZk(lockManager.norm(dest));
  }

  @Test
  public void testRenameDeeperToHigher() throws Exception {
    // Rename src to dest and validate that the znode for src is cleaned up.
    final Path src = TestUtils.testPath(hboss, "/a/b/1");
    final Path dest = TestUtils.testPath(hboss, "/a/1");
    assertTrue(hboss.mkdirs(src));
    // The src znode should exist after creating the dir in S3
    validatePathInZk(lockManager.norm(src));
    // mv /a/b/1 /a/1
    assertTrue(hboss.rename(src, dest));
    // We should not have a znode for the src
    validatePathNotInZk(lockManager.norm(src));
    // We should have a lock for the dest
    validatePathInZk(lockManager.norm(dest));
  }

  @Test
  public void testRenameHigherToDeeper() throws Exception {
    // Rename src to dest and validate that the znode for src is cleaned up.
    final Path src = TestUtils.testPath(hboss, "/a/1");
    final Path dest = TestUtils.testPath(hboss, "/a/b/1");
    // `mkdir /a/1`
    assertTrue(hboss.mkdirs(src));
    // The src znode should exist after creating the dir in S3
    validatePathInZk(lockManager.norm(src));
    // `mkdir /a/b`
    assertTrue(hboss.mkdirs(dest.getParent()));
    // `mv /a/1 /a/b/1`
    assertTrue(hboss.rename(src, dest));
    // We should have a znode for dest (we just locked it)
    validatePathNotInZk(lockManager.norm(src));
    // We should no longer have a znode for src (we effectively deleted it from S3)
    validatePathInZk(lockManager.norm(dest));
  }

  @Test
  public void testDelete() throws Exception {
    // Delete src and validate that the znode for src is cleaned up.
    final Path src = TestUtils.testPath(hboss, "src");
    assertTrue(hboss.mkdirs(src));
    // The src znode should exist after creating the dir in S3
    validatePathInZk(lockManager.norm(src));
    // `mv src dest`
    assertTrue(hboss.delete(src, true));
    // We should no longer have a znode for src since we deleted it from S3
    validatePathNotInZk(lockManager.norm(src));
  }
}
