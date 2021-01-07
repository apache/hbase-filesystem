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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.oss.Constants;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Implementation based on Apache Curator and Apache ZooKeeper. This allows
 * HBOSS to re-use an Apache HBase cluster's ZooKeeper ensemble for file
 * system locking.
 *
 * Can be enabled in JUnit tests with -Pzk. If {@link org.apache.hadoop.hbase.oss.Constants#ZK_CONN_STRING}
 * isn't specified, an embedded ZooKeeper process will be spun up for tests.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Unstable
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
    this.waitIntervalWarn = conf.getLong(Constants.WAIT_INTERVAL_WARN,
      TreeLockManager.DEFAULT_WAIT_INTERVAL_WARN);
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);

    // Create a temporary connection to ensure the root is created, then create
    // a new connection 'jailed' inside that root to eliminate the need for
    // paths to constantly be resolved inside the root.

    String zookeeperConnectionString = conf.get(Constants.ZK_CONN_STRING);
    // Fallback to the HBase ZK quorum.
    if (zookeeperConnectionString == null) {
      zookeeperConnectionString = conf.get("hbase.zookeeper.quorum");
    }
    curator = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
    curator.start();
    waitForCuratorToConnect();

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
    waitForCuratorToConnect();
  }

  private void waitForCuratorToConnect() {
    try {
      if (!requireNonNull(curator).blockUntilConnected(30, TimeUnit.SECONDS)) {
        throw new RuntimeException("Failed to connect to ZooKeeper");
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted waiting to connect to ZooKeeper", e);
      Thread.currentThread().interrupt();
      return;
    }
  }

  @Override
  public void close() {
    if (curator != null) {
      synchronized(this) {
        curator.close();
        lockCache.clear();
        logCaller();
      }
    }
  }

  private void logCaller(){
    if(LOG.isDebugEnabled()){
      StringBuilder builder = new StringBuilder();
      StackTraceElement[] elements = Thread.currentThread().getStackTrace();
      for(StackTraceElement e : elements) {
        builder.append(e.getClassName()).append(".").
          append(e.getMethodName()).append("(").append(e.getLineNumber()).append(")").append("\n");
      }
      LOG.debug("logging call with curator {} for instance {} at: {}",
        curator, this, builder.toString());
    }
  }

  @Override
  protected void writeLock(Path p) throws IOException {
    try {
      LOG.debug("writeLock {} acquire", p);
      get(p).writeLock().acquire();
    } catch (Exception e) {
      throw new IOException("Exception during write locking of path " + p, e);
    }
  }

  @Override
  protected void writeUnlock(Path p) throws IOException {
    try {
      LOG.debug("writeLock {} release", p);
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
    LOG.debug("readLock {} acquire", p);
    try {
      get(p).readLock().acquire();
    } catch (Exception e) {
      throw new IOException("Exception during read locking of path " + p, e);
    }
  }

  @Override
  protected void readUnlock(Path p) throws IOException {
    LOG.debug("readLock {} release", p);
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

  /*
  We need to protect this block against potential concurrent calls to close()
   */
  @Override
  protected synchronized boolean writeLockAbove(Path p) throws IOException {
    LOG.debug("Checking for write lock above {}", p);
    while (!p.isRoot()) {
      p = p.getParent();
      if (isLocked(get(p).writeLock())) {
        LOG.debug("Parent write lock currently held: {}", p);
        return true;
      }
    }
    return false;
  }

  @Override
  @InterfaceAudience.Private
  public boolean writeLockBelow(Path p, Depth depth) throws IOException {
    int maxLevel = (depth == Depth.DIRECTORY) ? 1 : Integer.MAX_VALUE;
    boolean b = writeLockBelow(p, 0, maxLevel);
    return b;
  }

  @Override
  @InterfaceAudience.Private
  public boolean readLockBelow(Path p, Depth depth) throws IOException {
    int maxLevel = (depth == Depth.DIRECTORY) ? 1 : Integer.MAX_VALUE;
    boolean b = readLockBelow(p, 0, maxLevel);
    return b;
  }

  @Override
  protected void recursiveDelete(Path p) throws IOException {
    try {
      ZKPaths.deleteChildren(curator.getZookeeperClient().getZooKeeper(),
            p.toString(), !p.isRoot());
      // Before this method is called, we have a guarantee that 
      //   1. There are no write locks above or below us
      //   2. There are no read locks below us
      // As such, we can just remove locks beneath us as we find them.
      removeInMemoryLocks(p);
    } catch (KeeperException.NoNodeException e) {
      LOG.warn("Lock not found during recursive delete: {}", p);
    } catch (Exception e) {
      throw new IOException("Exception while deleting lock " + p, e);
    }
  }

  private synchronized void removeInMemoryLocks(Path p) {
    Iterator<Entry<Path,InterProcessReadWriteLock>> iter = lockCache.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Path,InterProcessReadWriteLock> entry = iter.next();
      if (isBeneath(p, entry.getKey())) {
        LOG.trace("Removing lock for {}", entry.getKey());
        iter.remove();
      }
    }
  }

  private boolean isBeneath(Path parent, Path other) {
    if (parent.equals(other)) {
      return false;
    }
    // Is `other` fully contained in some path beneath the parent.
    return 0 == other.toString().indexOf(parent.toString());
  }

  private boolean writeLockBelow(Path p, int level, int maxLevel) throws IOException {
    try {
      if (level > 0 && isLocked(get(p).writeLock())) {
        return true;
      }
      if (level < maxLevel) {
        List<String> children = curator.getChildren().forPath(p.toString());
        for (String child : children) {
          if (child.equals(lockSubZnode)) {
            continue;
          }
          if (writeLockBelow(new Path(p, child), level+1, maxLevel)) {
            LOG.debug("Parent write lock currently held: {}", p);
            return true;
          }
        }
      }
    } catch (KeeperException.NoNodeException e) {
      // Ignore, means we hit the bottom of the tree
    } catch (Exception e) {
      throw new IOException("Error checking parents for write lock: " + p, e);
    }
    return false;
  }

  private boolean readLockBelow(Path p, int level, int maxLevel) throws IOException {
    try {
      if (level > 0 && isLocked(get(p).readLock())) {
        return true;
      }
      if (level < maxLevel) {
        List<String> children = curator.getChildren().forPath(p.toString());
        for (String child : children) {
          if (child.equals(lockSubZnode)) {
            continue;
          }
          if (readLockBelow(new Path(p, child), level+1, maxLevel)) {
            LOG.debug("Child read lock currently held: {}", p);
            return true;
          }
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
      return !lock.getParticipantNodes().isEmpty();
    } catch (KeeperException.NoNodeException e){
      return false;
    } catch (Exception e) {
      logCaller();
      throw new IOException("Exception while testing a lock", e);
    }
  }

  public String summarizeLocks() {
    StringBuilder sb = new StringBuilder();
    Map<Path,InterProcessReadWriteLock> cache = getUnmodifiableCache();
    for (Entry<Path,InterProcessReadWriteLock> entry : cache.entrySet()) {
      sb.append(entry.getKey()).append("=").append(describeLock(entry.getValue()));
    }
    return sb.toString();
  }

  String describeLock(InterProcessReadWriteLock lock) {
    if (lock == null) {
      return "null";
    }
    InterProcessMutex rlock = lock.readLock();
    InterProcessMutex wlock = lock.writeLock();
    StringBuilder sb = new StringBuilder();
    sb.append("ReadLock[heldByThisThread=").append(rlock.isOwnedByCurrentThread());
    sb.append(", heldInThisProcess=").append(rlock.isAcquiredInThisProcess()).append("]");
    sb.append(" WriteLock[heldByThisThread=").append(wlock.isOwnedByCurrentThread());
    sb.append(", heldInThisProcess=").append(wlock.isAcquiredInThisProcess()).append("]");
    return sb.toString();
  }

  public synchronized Map<Path,InterProcessReadWriteLock> getUnmodifiableCache() {
    return Collections.unmodifiableMap(lockCache);
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

