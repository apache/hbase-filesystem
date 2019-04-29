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

package org.apache.hadoop.hbase.oss.sync;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.Constants;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation based on Apache Curator and Apache ZooKeeper. This allows
 * HBOSS to re-use an Apache HBase cluster's ZooKeeper ensemble for file
 * system locking.
 *
 * Can be enabled in JUnit tests with -Pzk. If {@link Constants.ZK_CONN_STRING}
 * isn't specified, an embedded ZooKeeper process will be spun up for tests.
 */
public class ZKTreeLockManager extends TreeLockManager {
  private static final Logger LOG =
        LoggerFactory.getLogger(ZKTreeLockManager.class);

  private CuratorFramework curator;

  private String root;

  private void setRoot() {
    root = "/hboss";
  }

  private static final String lockSubZnode = ".hboss-lock-znode";

  private Map<Path,InterProcessReadWriteLock> lockCache = new HashMap<>();

  public void initialize(FileSystem fs) throws IOException {
    this.fs = fs;
    Configuration conf = fs.getConf();
    int baseSleepTimeMs = conf.getInt(Constants.ZK_BASE_SLEEP_MS, 1000);
    int maxRetries = conf.getInt(Constants.ZK_MAX_RETRIES, 3);
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);

    // Create a temporary connection to ensure the root is created, then create
    // a new connection 'jailed' inside that root to eliminate the need for
    // paths to constantly be resolved inside the root.

    String zookeeperConnectionString = conf.get(Constants.ZK_CONN_STRING);
    curator = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
    curator.start();

    setRoot();
    try {
      ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), root, true);
    } catch (Exception e) {
      throw new IOException("Unable to initialize root znodes", e);
    }
    curator.close();

    zookeeperConnectionString += root;
    curator = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
    curator.start();
  }

  @Override
  public void close() throws IOException {
    curator.close();
  }

  @Override
  protected void writeLock(Path p) throws IOException {
    try {
      get(p).writeLock().acquire();
    } catch (Exception e) {
      throw new IOException("Exception during write locking of path " + p, e);
    }
  }

  @Override
  protected void writeUnlock(Path p) throws IOException {
    try {
      get(p).writeLock().release();
    } catch(IllegalMonitorStateException e) {
      // Reentrant locks might be acquired multiple times
      LOG.error("Tried to release unacquired write lock: {}", p);
      throw e;
    } catch (Exception e) {
      throw new IOException("Exception during write unlocking of path " + p, e);
    }
  }

  @Override
  protected void readLock(Path p) throws IOException {
    try {
      get(p).readLock().acquire();
    } catch (Exception e) {
      throw new IOException("Exception during read locking of path " + p, e);
    }
  }

  @Override
  protected void readUnlock(Path p) throws IOException {
    try {
      get(p).readLock().release();
    } catch(IllegalMonitorStateException e) {
      // Reentrant locks might be acquired multiple times
      LOG.error("Tried to release unacquired write lock: {}", p);
      throw e;
    } catch (Exception e) {
      throw new IOException("Exception during read unlocking of path " + p, e);
    }
  }

  @Override
  protected boolean writeLockAbove(Path p) throws IOException {
    while (!p.isRoot()) {
      p = p.getParent();
      if (isLocked(get(p).writeLock())) {
        LOG.warn("Parent write lock currently held: {}", p);
        return true;
      }
    }
    return false;
  }

  @Override
  protected boolean writeLockBelow(Path p) throws IOException {
    boolean b = writeLockBelow(p, true);
    return b;
  }

  @Override
  protected boolean readLockBelow(Path p) throws IOException {
    boolean b = readLockBelow(p, true);
    return b;
  }

  @Override
  protected void recursiveDelete(Path p) throws IOException {
    try {
      ZKPaths.deleteChildren(curator.getZookeeperClient().getZooKeeper(),
            p.toString(), !p.isRoot());
    } catch (KeeperException.NoNodeException e) {
      LOG.warn("Lock not found during recursive delete: {}", p);
    } catch (Exception e) {
      throw new IOException("Exception while deleting lock " + p, e);
    }
  }

  private boolean writeLockBelow(Path p, boolean firstLevel) throws IOException {
    try {
      if (!firstLevel && isLocked(get(p).writeLock())) {
        return true;
      }
      List<String> children = curator.getChildren().forPath(p.toString());
      for (String child : children) {
        if (child.equals(lockSubZnode)) {
          continue;
        }
        if (writeLockBelow(new Path(p, child), false)) {
          LOG.warn("Parent write lock currently held: {}", p);
          return true;
        }
      }
    } catch (KeeperException.NoNodeException e) {
      // Ignore, means we hit the bottom of the tree
    } catch (Exception e) {
      throw new IOException("Error checking parents for write lock: " + p, e);
    }
    return false;
  }

  private boolean readLockBelow(Path p, boolean firstLevel) throws IOException {
    try {
      if (!firstLevel && isLocked(get(p).readLock())) {
        return true;
      }
      List<String> children = curator.getChildren().forPath(p.toString());
      for (String child : children) {
        if (child.equals(lockSubZnode)) {
          continue;
        }
        if (readLockBelow(new Path(p, child), false)) {
          LOG.warn("Child read lock currently held: {}", p);
          return true;
        }
      }
    } catch (KeeperException.NoNodeException e) {
      // Ignore, means we hit the bottom of the tree
    } catch (Exception e) {
      throw new IOException("Error checking children for read lock: " + p, e);
    }
    return false;
  }

  /**
   * Specifically, if this is lock by another thread.
   */
  private boolean isLocked(InterProcessMutex lock) throws IOException {
    try {
      if (lock.isOwnedByCurrentThread()) {
        // First check the current thread, because we allow you to get locks
        // when parent or child paths are only locked by you.
        return false;
      }
      if (lock.isAcquiredInThisProcess()) {
        // We know it's not this thread, but this is less expensive
        // than checking other processes.
        return true;
      }
      // Finally, see if another process holds the lock. This is a terrible way
      // to check but Curator doesn't expose another way.
      if (lock.acquire(0, TimeUnit.NANOSECONDS)) {
        lock.release();
        return false;
      }
    } catch (Exception e) {
      throw new IOException("Exception while testing a lock", e);
    }
    return true;
  }

  private synchronized InterProcessReadWriteLock get(Path path) throws IOException {
    if (!lockCache.containsKey(path)) {
      String zkPath = new Path(path, lockSubZnode).toString();
      try {
        ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), zkPath, true);
      } catch (KeeperException.NodeExistsException e) {
        // Ignore
      } catch (Exception e) {
        throw new IOException("Exception while ensuring lock parents exist: " +
              path, e);
      }
      lockCache.put(path, new InterProcessReadWriteLock(curator, zkPath));
    }
    return lockCache.get(path);
  }
}

