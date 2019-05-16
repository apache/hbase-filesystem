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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation based on standard Java concurrency primitives. This makes it
 * useless for anything but testing, development, and maybe a single-node HBase
 * instance. It is however much faster and easier to debug, despite including
 * logic very similar to what other implementations would need.
 *
 * Can be enabled in JUnit tests with -Plocal (although it's the default).
 */
public class LocalTreeLockManager extends TreeLockManager {
  private static final Logger LOG =
        LoggerFactory.getLogger(LocalTreeLockManager.class);

  public void initialize(FileSystem fs) {
    this.fs = fs;
  }

  @Override
  protected void writeLock(Path p) throws IOException {
    createLocksIfNeeded(p);
    index.get(p).lock.writeLock().lock();
  }

  @Override
  protected void writeUnlock(Path p) throws IOException {
    try {
      LockNode node = index.get(p);
      // Node to unlock may already be gone after deletes
      if (node != null) {
        node.lock.writeLock().unlock();
      }
    } catch(IllegalMonitorStateException e) {
      // Reentrant locks might be acquired multiple times
      LOG.error("Tried to release unacquired write lock: {}", p);
      throw e;
    }
  }

  @Override
  protected void readLock(Path p) throws IOException {
    createLocksIfNeeded(p);
    index.get(p).lock.readLock().lock();
  }

  @Override
  protected void readUnlock(Path p) throws IOException {
    try {
      index.get(p).lock.readLock().unlock();
    } catch(IllegalMonitorStateException e) {
      // Reentrant locks might be acquired multiple times
      LOG.error("Tried to release unacquired read lock: {}", p);
      throw e;
    }
  }

  @Override
  protected boolean writeLockAbove(Path p) throws IOException {
    createLocksIfNeeded(p);
    while (!p.isRoot()) {
      p = p.getParent();
      LockNode currentNode = index.get(p);
      if (currentNode.lock.isWriteLocked() &&
            !currentNode.lock.isWriteLockedByCurrentThread()) {
        LOG.warn("Parent write lock currently held: {}", p);
        return true;
      }
    }
    return false;
  }

  @Override
  @VisibleForTesting
  public boolean writeLockBelow(Path p, Depth depth) throws IOException {
    createLocksIfNeeded(p);
    int maxLevel = (depth == Depth.DIRECTORY) ? 1 : Integer.MAX_VALUE;
    return writeLockBelow(p, 0, maxLevel);
  }

  @Override
  @VisibleForTesting
  public boolean readLockBelow(Path p, Depth depth) throws IOException {
    createLocksIfNeeded(p);
    int maxLevel = (depth == Depth.DIRECTORY) ? 1 : Integer.MAX_VALUE;
    return readLockBelow(p, 0, maxLevel);
  }

  @Override
  protected void recursiveDelete(Path p) {
    if (!p.isRoot()) {
      index.get(p.getParent()).children.remove(p);
    }
    LockNode currentNode = index.get(p);
    innerRecursiveDelete(p);
    currentNode.lock.writeLock().unlock();
  }

  private void innerRecursiveDelete(Path p) {
    LockNode currentNode = index.remove(p);
    Set<Path> childPaths = currentNode.children.keySet();
    for (Path child : childPaths) {
      innerRecursiveDelete(child);
    }
  }

  private class LockNode {
    public Path path;

    public LockNode(Path path) {
      this.path = path;
    }

    public Map<Path, LockNode> children = new HashMap<>();
    public ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  }

  private Map<Path, LockNode> index = new HashMap<>();

  private synchronized void createLocksIfNeeded(Path p) {
    if (index.containsKey(p)) {
      return;
    }
    Path lastPath = null;
    while (p != null) {
      LockNode currentNode = index.get(p);
      if (currentNode != null) {
        if (lastPath != null) {
          currentNode.children.put(lastPath, index.get(lastPath));
        }
        return;
      }
      currentNode = new LockNode(p);
      if (lastPath != null) {
        currentNode.children.put(lastPath, index.get(lastPath));
      }
      index.put(p, currentNode);
      lastPath = p;
      p = p.getParent();
    }
  }

  private synchronized boolean writeLockBelow(Path p, int level, int maxLevel) {
    LockNode currentNode = index.get(p);
    if (level > 0 && currentNode.lock.isWriteLocked() &&
          !currentNode.lock.isWriteLockedByCurrentThread()) {
      LOG.warn("Child write lock current held: {}", p);
      return true;
    }
    if (level <= maxLevel) {
      Set<Path> childPaths = currentNode.children.keySet();
      for (Path child : childPaths) {
        if (writeLockBelow(child, level+1, maxLevel)) {
          return true;
        }
      }
    }
    return false;
  }

  // TODO will return true even if current thread has a read lock below...
  private synchronized boolean readLockBelow(Path p, int level, int maxLevel) {
    LockNode currentNode = index.get(p);
    if (level > 0 && currentNode.lock.getReadLockCount() > 0) {
      LOG.warn("Child read lock currently held: {}", p);
      return true;
    }
    if (level <= maxLevel) {
      Set<Path> childPaths = index.get(p).children.keySet();
      for (Path child : childPaths) {
        if (readLockBelow(child, level+1, maxLevel)) {
          return true;
        }
      }
    }
    return false;
  }
}

