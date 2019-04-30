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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.Constants;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic logic for synchronizing FileSystem operations. Needs to be extended
 * with an implementation of read / write locks and the methods to check the
 * status of locks above and below FileSystem Paths.
 */
@InterfaceAudience.LimitedPrivate({"HBase"})
@InterfaceStability.Unstable
public abstract class TreeLockManager {
  private static final Logger LOG =
        LoggerFactory.getLogger(TreeLockManager.class);

  public static synchronized TreeLockManager get(FileSystem fs)
        throws IOException {
    Configuration conf = fs.getConf();
    Class<?> impl = conf.getClass(Constants.SYNC_IMPL, TreeLockManager.class); // <?>
    TreeLockManager instance = null;
    Exception cause = null;
    try {
      instance = (TreeLockManager)impl.newInstance();
    } catch (Exception e) {
      cause = e;
    }
    if (instance == null) {
      throw new IOException("Class referred to by "
            + Constants.SYNC_IMPL + ", " + impl.getName()
            + ", is not a valid implementation of "
            + TreeLockManager.class.getName(), cause);
    }
    instance.initialize(fs);
    return instance;
  }

  protected FileSystem fs;

  private static final String SLASH = "/";

  /**
   * Returns a normalized logical path that uniquely identifies an object-store
   * "filesystem" and a path inside it. Assumes a 1:1 mapping between hostnames
   * and filesystems, and assumes the URI scheme mapping is consistent
   * everywhere.
   */
  private Path norm(Path path) {
    URI uri = fs.makeQualified(path).toUri();
    String uriScheme = uri.getScheme();
    String uriHost = uri.getHost();
    String uriPath = uri.getPath().substring(1);
    if (uriPath.length() == 0) {
      uriPath = SLASH;
    }
    // To combine fsRoot and uriPath, fsRoot must start with /, and uriPath
    // must not.
    Path fsRoot = new Path(SLASH + uriScheme, uriHost);
    return new Path(fsRoot, uriPath);
  }

  /**
   * Convenience function for calling norm on an array. Returned copy of the
   * array will also be sorted for deadlock avoidance.
   */
  private Path[] norm(Path[] paths) {
    Path[] newPaths = new Path[paths.length];
    for (int i = 0; i < paths.length; i++) {
      newPaths[i] = norm(paths[i]);
    }
    Arrays.sort(newPaths);
    return newPaths;
  }

  /**
   * Convenience function for calling norm on an array with one extra arg.
   * Returned copy of the combined array will also be sorted for deadlock
   * avoidance.
   */
  private Path[] norm(Path[] paths, Path path) {
    Path[] newPaths = new Path[paths.length + 1];
    int i;
    for (i = 0; i < paths.length; i++) {
      newPaths[i] = norm(paths[i]);
    }
    newPaths[i] = path;
    Arrays.sort(newPaths);
    return newPaths;
  }

  /**
   * In addition to any implementation-specific setup, implementations must set
   * this.fs = fs in order for path normalization to work.
   */
  public abstract void initialize(FileSystem fs) throws IOException;

  /**
   * Performs any shutdown necessary when a client is exiting. Should be
   * considered best-effort and for planned shut downs.
   */
  public void close() throws IOException {
  }

  /**
   * Acquires a single exclusive (write) lock.
   *
   * @param p Path to lock
   */
  protected abstract void writeLock(Path p) throws IOException;

  /**
   * Releases a single exclusive (write) lock.
   *
   * @param p Path to unlock
   */
  protected abstract void writeUnlock(Path p) throws IOException;

  /**
   * Acquires a single non-exclusive (read) lock.
   *
   * @param p Path to lock
   */
  protected abstract void readLock(Path p) throws IOException;

  /**
   * Releases a single non-exclusive (read) lock.
   *
   * @param p Path to unlock
   */
  protected abstract void readUnlock(Path p) throws IOException;

  /**
   * Checks for the presence of a write lock on all parent directories of the
   * path.
   *
   * @param p Path to check
   * @return True if a lock is found, false otherwise
   */
  protected abstract boolean writeLockAbove(Path p) throws IOException;

  /**
   * Checks for the presence of a write lock on all children of the path.
   *
   * @param p Path to check
   * @return True if a lock is found, false otherwise
   */
  protected abstract boolean writeLockBelow(Path p) throws IOException;

  /**
   * Checks for the presence of a write lock on all child directories of the
   * path.
   *
   * @param p Path to check
   * @return True if a lock is found, false otherwise
   */
  protected abstract boolean readLockBelow(Path p) throws IOException;

  /**
   * Recursively cleans up locks that won't be used again.
   *
   * @param p Parent path of all locks to delete
   */
  protected abstract void recursiveDelete(Path p) throws IOException;

  private RetryPolicy retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(600000, 1, TimeUnit.MILLISECONDS);

  private boolean retryBackoff(int retries) throws IOException {
    RetryAction action;
    try {
      action = retryPolicy.shouldRetry(null, retries, 0, true);
    } catch (Exception e) {
      throw new IOException("Unexpected exception during locking", e);
    }
    if (action.action == RetryDecision.FAIL) {
      throw new IOException("Exceeded " + retries + " retries for locking");
    }
    LOG.trace("Sleeping {}ms before next retry", action.delayMillis);
    try {
      Thread.sleep(action.delayMillis);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted during locking", e);
    }
    return true;
  }

  /**
   * Acquires a write (exclusive) lock on a path. Between this lock being
   * acquired and being released, we should hold a write lock on this path, no
   * write locks should be held by anyone on any parent directory, and no read
   * or write locks should be held by anyone on any child directory.
   *
   * @param path Path to lock
   */
  protected void treeWriteLock(Path p) throws IOException {
    int outerRetries = 0;
    do {
      int innerRetries = 0;
      do {
        // If there's already a write-lock above or below us in the tree, wait for it to leave
        if (writeLockAbove(p) || writeLockBelow(p)) {
          LOG.warn("Blocked on some parent write lock, waiting: {}", p);
          continue;
        }
        break;
      } while (retryBackoff(innerRetries++));
      // Try obtain the write lock just for our node
      writeLock(p);
      // If there's now a write-lock above or below us in the tree, release and retry
      if (writeLockAbove(p) || writeLockBelow(p)) {
        LOG.warn("Blocked on some other write lock, retrying: {}", p);
        writeUnlock(p);
        continue;
      }
      break;
    } while (retryBackoff(outerRetries++));

    // Once we know we're the only write-lock in our path, drain all read-locks below
    int drainReadLocksRetries = 0;
    do {
      if (readLockBelow(p)) {
        LOG.warn("Blocked on some child read lock, writing: {}", p);
        continue;
      }
      break;
    } while (retryBackoff(drainReadLocksRetries++));
  }

  /**
   * Acquires a read (non-exclusive) lock on a path. Between this lock being
   * acquired and being released, we should hold a read lock on this path, and
   * no write locks should be held by anyone on any parent directory.
   *
   * @param path Path to lock
   */
  protected void treeReadLock(Path p) throws IOException {
    int outerRetries = 0;
    do {
      int innerRetries = 0;
      do {
        // If there's a write lock above us, wait
        if (writeLockAbove(p)) {
          LOG.warn("Blocked waiting for some parent write lock, waiting: {}",
                p);
          continue;
        }
        break;
      } while (retryBackoff(innerRetries++));
      // Try obtain the read-lock just for our node
      readLock(p);
      // If there's a write lock above us, release the lock and try again
      if (writeLockAbove(p)) {
        LOG.warn("Blocked waiting for some parent write lock, retrying: {}",
              p);
        readUnlock(p);
        continue;
      }
      break;
    } while (retryBackoff(outerRetries++));
  }

  /**
   * Acquires an exclusive lock on a single path to create or append to a file.
   * This is required for createNonRecursive() as well as other operations to be
   * atomic because the underlying file may not be created until all data has
   * been written.
   *
   * @param path Path of the create operation
   * @return AutoLock to release this path
   */
  public AutoLock lockWrite(Path rawPath) throws IOException {
    Path path = norm(rawPath);
    LOG.debug("About to lock for create / write: {}", rawPath);
    treeWriteLock(path);
    return new AutoLock() {
      public void close() throws IOException {
        LOG.debug("About to unlock after create / write: {}", path);
        writeUnlock(path);
      }
    };
  }

  /**
   * Acquires an exclusive lock on a single path and then cleans up the lock
   * and those of all children. The lock ensures this doesn't interfere with any
   * renames or other listing operations above this path.
   *
   * @param path Path of the create operation
   * @return AutoLock to release this path
   */
  public AutoLock lockDelete(Path rawPath) throws IOException {
    Path path = norm(rawPath);
    LOG.debug("About to lock for delete: {}", path);
    treeWriteLock(path);
    return new AutoLock() {
      public void close() throws IOException {
        LOG.debug("About to recursively delete locks: {}", path);
        recursiveDelete(path);
      }
    };
  }

  /**
   * Acquires a lock on a single path to run a listing operation. We need to
   * ensure that the listing is not generated mid-rename. This will lock all
   * children of the root path as well, which is only necessary for recursive
   * listings. Other listings should only need a read lock on the root and all
   * children, but that is not implemented.
   *
   * @param path Root of the listing operation
   * @return AutoCloseable to release this path
   */
  public AutoLock lockListing(Path rawPath) throws IOException {
    Path path = norm(rawPath);
    LOG.debug("About to lock for listing: {}", path);
    treeWriteLock(path);
    return new AutoLock() {
      public void close() throws IOException {
        LOG.debug("About to unlock after listing: {}", path);
        writeUnlock(path);
      }
    };
  }

  /**
   * Same considerations of lockListing, but locks an array of paths in order
   * and returns an AutoLock that encapsulates all of them.
   *
   * @param paths
   * @return AutoCloseable that encapsulate all paths
   */
  public AutoLock lockListings(Path[] rawPaths) throws IOException {
    Path[] paths = norm(rawPaths);
    for (int i = 0; i < paths.length; i++) {
      LOG.debug("About to lock for listings: {}", paths[i]);
      treeWriteLock(paths[i]);
    }
    return new AutoLock() {
      public void close() throws IOException {
        Throwable lastThrown = null;
        for (int i = 0; i < paths.length; i++) {
          LOG.debug("About to unlock after listings: {}", paths[i]);
          try {
            writeUnlock(paths[i]);
          } catch (Throwable e) {
            lastThrown = e;
            LOG.warn("Caught throwable while unlocking: {}", e.getMessage());
            e.printStackTrace();
          }
        }
        if (lastThrown != null) {
          throw new IOException("At least one throwable caught while unlocking",
                lastThrown);
        }
      }
    };
  }

  /**
   * Acquires an exclusive (write) lock on 2 paths, for a rename. Any given pair
   * of paths will always be locked in the same order, regardless of their order
   * in the method call. This is to avoid deadlocks. In the future this method
   * may also record the start of the rename in something like a write-ahead log
   * to recover in-progress renames in the event of a failure.
   *
   * @param src Source of the rename
   * @param dst Destination of the rename
   * @return AutoCloseable to release both paths
   * @throws IOException
   */
  public AutoLock lockRename(Path rawSrc, Path rawDst) throws IOException {
    Path src = norm(rawSrc);
    Path dst = norm(rawDst);
    LOG.debug("About to lock for rename: from {} to {}", src, dst);
    if (src.compareTo(dst) < 0) {
      treeWriteLock(src);
      treeWriteLock(dst);
    } else {
      treeWriteLock(dst);
      treeWriteLock(src);
    }
    return new AutoLock() {
      public void close() throws IOException {
        LOG.debug("About to unlock after rename: from {} to {}", src, dst);
        try {
          writeUnlock(src);
        } finally {
          writeUnlock(dst);
        }
      }
    };
  }

  /**
   * Returns a non-exclusive lock on a single path. This is for generic cases
   * that read or modify the file-system but that don't necessarily need
   * exclusive access if no other concurrent operations do.
   *
   * @param path Path to lock
   * @return AutoCloseable that will release the path
   * @throws IOException
   */
  public AutoLock lock(Path rawPath) throws IOException {
    Path path = norm(rawPath);
    LOG.debug("About to lock: {}", path);
    treeReadLock(path);
    return new AutoLock() {
      public void close() throws IOException {
        LOG.debug("About to unlock: {}", path);
        readUnlock(path);
      }
    };
  }

  /**
   * Returns a non-exclusive lock on an array of paths. This is for generic
   * cases that read or modify the file-system but that don't necessarily need
   * exclusive access if no other concurrent operations do.
   *
   * @param paths Path to lock
   * @return AutoCloseable that will release all the paths
   * @throws IOException
   */
  public AutoLock lock(Path[] rawPaths) throws IOException {
    return innerLock(norm(rawPaths));
  }

  /**
   * Returns a non-exclusive lock on an array of paths and a separate path. No
   * distinction is made between them in locking: this method is only for
   * convenience in the FileSystem implementation where there is a distinction.
   *
   * @param extraPath Extra path to lock
   * @param paths Paths to lock
   * @return AutoCloseable that will release all the paths
   * @throws IOException
   */
  public AutoLock lock(Path extraPath, Path[] rawPaths) throws IOException {
    return innerLock(norm(rawPaths, extraPath));
  }

  private AutoLock innerLock(Path[] paths) throws IOException {
    for (int i = 0; i < paths.length; i++) {
      LOG.debug("About to lock: {}", paths[i]);
      treeReadLock(paths[i]);
    }
    return new AutoLock() {
      public void close() throws IOException {
        Throwable lastThrown = null;
        for (int i = 0; i < paths.length; i++) {
          LOG.debug("About to unlock: {}", paths[i]);
          try {
            readUnlock(paths[i]);
          } catch (Throwable e) {
            lastThrown = e;
            LOG.warn("Caught throwable while unlocking: {}", e.getMessage());
            e.printStackTrace();
          }
        }
        if (lastThrown != null) {
          throw new IOException("At least one throwable caught while unlocking",
                lastThrown);
        }
      }
    };
  }
}
