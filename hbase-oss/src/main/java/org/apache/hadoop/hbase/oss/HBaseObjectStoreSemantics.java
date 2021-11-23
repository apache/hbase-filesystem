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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.oss.metrics.MetricsOSSSource;
import org.apache.hadoop.hbase.oss.metrics.MetricsOSSSourceImpl;
import org.apache.hadoop.hbase.oss.sync.AutoLock;
import org.apache.hadoop.hbase.oss.sync.AutoLock.LockedFSDataOutputStream;
import org.apache.hadoop.hbase.oss.sync.AutoLock.LockedRemoteIterator;
import org.apache.hadoop.hbase.oss.sync.LockedFutureDataInputStreamBuilder;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager.Depth;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A FileSystem implementation that layers locking logic on top of another,
 * underlying implementation. The appropriate lock on a path is acquired before
 * passing on the call, and is either released upon returning or the resulting
 * data stream / iterator takes ownership of the lock until it is closed. Newer
 * features of FileSystem may either be unimplemented or will not be aware of
 * the locks. Caveats with existing features include:
 * <ul>
 *   <li>
 *     The deleteOnExit feature has no locking logic beyond the underlying
 *     exists() and delete() calls they make. Shouldn't be a problem due to the
 *     documented best-effort nature of the feature.
 *   </li>
 *   <li>
 *     globStatus isn't even atomic on HDFS, as it does a tree-walk and may do
 *     multiple independent requests to the NameNode. This is potentially worse
 *     on object-stores where latency is higher, etc. Currently globStatus is
 *     only used for the CoprocessorClassLoader and listing table directories.
 *     These operations are not considered sensitive to atomicity. Globbing
 *     could be made atomic by getting a write lock on the parent of the first
 *     wildcard.
 *   </li>
 *   <li>
 *     Symlinking is not supported, but not used by HBase at all and not
 *     supported by mainstream object-stores considered in this design.
 *   </li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Unstable
public class HBaseObjectStoreSemantics extends FilterFileSystem {
  private static final Logger LOG =
        LoggerFactory.getLogger(HBaseObjectStoreSemantics.class);

  private TreeLockManager sync;
  private MetricsOSSSource metrics;

  public void initialize(URI name, Configuration conf) throws IOException {
    setConf(conf);

    String scheme = name.getScheme();
    String schemeImpl = "fs." + scheme + ".impl";
    String hbossSchemeImpl = "fs.hboss." + schemeImpl;
    String wrappedImpl = conf.get(hbossSchemeImpl);
    Configuration internalConf = new Configuration(conf);

    if (wrappedImpl != null) {
      LOG.info("HBOSS wrapping file-system {} using implementation {}", name,
            wrappedImpl);
      String disableCache = "fs." + scheme + ".impl.disable.cache";
      internalConf.set(schemeImpl, wrappedImpl);
      internalConf.set(disableCache, "true");
    }

    fs = FileSystem.get(name, internalConf);
    sync = TreeLockManager.get(fs);
    metrics = MetricsOSSSourceImpl.getInstance();
  }

  @InterfaceAudience.Private
  TreeLockManager getLockManager() {
    return sync;
  }

  public String getScheme() {
    return fs.getScheme();
  }

  @Override
  public String getCanonicalServiceName() {
    return fs.getCanonicalServiceName();
  }

  @Deprecated
  public String getName() {
    return fs.getName();
  }

  public BlockLocation[] getFileBlockLocations(FileStatus file,
                                               long start, long len) throws IOException {
    try (AutoLock l = sync.lock(file.getPath())) {
      return fs.getFileBlockLocations(file, start, len);
    }
  }

  public BlockLocation[] getFileBlockLocations(Path p,
                                               long start, long len) throws IOException {
    try (AutoLock l = sync.lock(p)) {
      return fs.getFileBlockLocations(p, start, len);
    }
  }

  public FSDataInputStream open(Path f, int bufferSize)
        throws IOException {
    try (AutoLock l = sync.lock(f)) {
      return fs.open(f, bufferSize);
    }
  }

  public FSDataInputStream open(Path f) throws IOException {
    try (AutoLock l = sync.lock(f)) {
      return fs.open(f);
    }
  }

  @Override
  public FutureDataInputStreamBuilder openFile(final Path path)
      throws IOException, UnsupportedOperationException {
    return new LockedFutureDataInputStreamBuilder(sync, path,
        fs.openFile(path));
  }

  /**
   * This is mostly unsupported, and as there's no way to
   * get the path from a pathHandle, impossible to lock.
   * @param pathHandle path
   * @return never returns successfully.
   * @throws UnsupportedOperationException always
   */
  @Override
  public FutureDataInputStreamBuilder openFile(final PathHandle pathHandle)
      throws IOException, UnsupportedOperationException {
    throw new UnsupportedOperationException("openFile(PathHandle) unsupported");
  }

  public FSDataOutputStream create(Path f) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream create(Path f, boolean overwrite)
        throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, overwrite);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream create(Path f, Progressable progress)
        throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, progress);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream create(Path f, short replication)
        throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, replication);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream create(Path f, short replication,
                                   Progressable progress) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, replication, progress);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream create(Path f,
                                   boolean overwrite,
                                   int bufferSize
  ) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, overwrite, bufferSize);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream create(Path f,
                                   boolean overwrite,
                                   int bufferSize,
                                   Progressable progress
  ) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, overwrite, bufferSize, progress);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream create(Path f,
                                   boolean overwrite,
                                   int bufferSize,
                                   short replication,
                                   long blockSize) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, overwrite, bufferSize,
            replication, blockSize);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream create(Path f,
                                   boolean overwrite,
                                   int bufferSize,
                                   short replication,
                                   long blockSize,
                                   Progressable progress
  ) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, overwrite, bufferSize,
            replication, blockSize, progress);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream create(Path f,
                                   FsPermission permission,
                                   boolean overwrite,
                                   int bufferSize,
                                   short replication,
                                   long blockSize,
                                   Progressable progress) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, permission, overwrite,
            bufferSize, replication, blockSize, progress);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream create(Path f,
                                   FsPermission permission,
                                   EnumSet<CreateFlag> flags,
                                   int bufferSize,
                                   short replication,
                                   long blockSize,
                                   Progressable progress) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, permission, flags, bufferSize,
            replication, blockSize, progress);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream create(Path f,
                                   FsPermission permission,
                                   EnumSet<CreateFlag> flags,
                                   int bufferSize,
                                   short replication,
                                   long blockSize,
                                   Progressable progress,
                                   ChecksumOpt checksumOpt) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, permission, flags, bufferSize,
            replication, blockSize, progress, checksumOpt);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream createNonRecursive(Path f,
                                               boolean overwrite,
                                               int bufferSize, short replication, long blockSize,
                                               Progressable progress) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, overwrite, bufferSize,
            replication, blockSize, progress);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
                                               boolean overwrite, int bufferSize, short replication, long blockSize,
                                               Progressable progress) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, permission, overwrite,
            bufferSize, replication, blockSize, progress);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
                                               EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
                                               Progressable progress) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.create(f, permission, flags, bufferSize,
            replication, blockSize, progress);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public boolean createNewFile(Path f) throws IOException {
    try (AutoLock l = sync.lockWrite(f)) {
      return fs.createNewFile(f);
    }
  }

  public FSDataOutputStream append(Path f) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.append(f);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.append(f, bufferSize);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public FSDataOutputStream append(Path f, int bufferSize,
                                   Progressable progress) throws IOException {
    AutoLock lock = sync.lockWrite(f);
    try {
      FSDataOutputStream stream = fs.append(f, bufferSize, progress);
      return new LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw(e);
    }
  }

  public void concat(final Path trg, final Path[] psrcs) throws IOException {
    try (AutoLock l = sync.lock(trg, psrcs)) {
      fs.concat(trg, psrcs);
    }
  }

  @Deprecated
  public short getReplication(Path src) throws IOException {
    try (AutoLock l = sync.lock(src)) {
      return fs.getReplication(src);
    }
  }

  public boolean setReplication(Path src, short replication)
        throws IOException {
    try (AutoLock l = sync.lock(src)) {
      return fs.setReplication(src, replication);
    }
  }

  public boolean rename(Path src, Path dst) throws IOException {
    long startTime = System.currentTimeMillis();
    long lockAcquiredTime = startTime;
    long doneTime = startTime;
    // Future to pass into the AutoLock so it knows if it should clean up.
    final CompletableFuture<Boolean> renameResult = new CompletableFuture<>();
    try (AutoLock l = sync.lockRename(src, dst, renameResult)) {
      lockAcquiredTime = System.currentTimeMillis();
      metrics.updateAcquireRenameLockHisto(lockAcquiredTime- startTime);
      // Defaulting to false in the case that fs.rename throws an exception
      boolean result = false;
      try {
        result = fs.rename(src, dst);
        return result;
      } finally {
        renameResult.complete(result);
        doneTime = System.currentTimeMillis();
        metrics.updateRenameFsOperationHisto(doneTime - lockAcquiredTime);
      }
    }
    finally {
      long releasedLocksTime = System.currentTimeMillis();
      metrics.updateReleaseRenameLockHisto(releasedLocksTime - doneTime);
    }
  }

  public boolean truncate(Path f, long newLength) throws IOException {
    try (AutoLock l = sync.lock(f)) {
      return fs.truncate(f, newLength);
    }
  }

  @Deprecated
  public boolean delete(Path f) throws IOException {
    try (AutoLock l = sync.lockDelete(f)) {
      return fs.delete(f);
    }
  }

  public boolean delete(Path f, boolean recursive) throws IOException {
    try (AutoLock l = sync.lockDelete(f)) {
      return fs.delete(f, recursive);
    }
  }

  public boolean exists(Path f) throws IOException {
    try (AutoLock l = sync.lock(f)) {
      try {
        return fs.exists(f);
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      }
    }
  }

  @Deprecated
  public boolean isDirectory(Path f) throws IOException {
    try (AutoLock l = sync.lock(f)) {
      return fs.isDirectory(f);
    }
  }

  @Deprecated
  public boolean isFile(Path f) throws IOException {
    try (AutoLock l = sync.lock(f)) {
      return fs.isFile(f);
    }
  }

  @Deprecated
  public long getLength(Path f) throws IOException {
    try (AutoLock l = sync.lock(f)) {
      return fs.getLength(f);
    }
  }

  public ContentSummary getContentSummary(Path f) throws IOException {
    try (AutoLock l = sync.lock(f)) {
      return fs.getContentSummary(f);
    }
  }

  public FileStatus[] listStatus(Path f) throws FileNotFoundException,
        IOException {
    try (AutoLock l = sync.lockListing(f, Depth.DIRECTORY)) {
      return fs.listStatus(f);
    }
  }

  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
        throws IOException {
    try (AutoLock l = sync.lock(path)) {
      return fs.listCorruptFileBlocks(path);
    }
  }

  public FileStatus[] listStatus(Path f, PathFilter filter)
        throws FileNotFoundException, IOException {
    try (AutoLock l = sync.lockListing(f, Depth.DIRECTORY)) {
      return fs.listStatus(f, filter);
    }
  }

  public FileStatus[] listStatus(Path[] files)
        throws FileNotFoundException, IOException {
    try (AutoLock l = sync.lockListings(files, Depth.DIRECTORY)) {
      return fs.listStatus(files);
    }
  }

  public FileStatus[] listStatus(Path[] files, PathFilter filter)
        throws FileNotFoundException, IOException {
    try (AutoLock l = sync.lockListings(files, Depth.DIRECTORY)) {
      return fs.listStatus(files, filter);
    }
  }

  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    LOG.warn("Globbing is never atomic!");
    return fs.globStatus(pathPattern);
  }

  public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
        throws IOException {
    LOG.warn("Globbing is never atomic!");
    return fs.globStatus(pathPattern, filter);
  }

  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
        throws FileNotFoundException, IOException {
    AutoLock lock = sync.lockListing(f, Depth.DIRECTORY);
    try {
      RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(f);
      return new LockedRemoteIterator<LocatedFileStatus>(iterator, lock);
    } catch (Exception e) {
      lock.close();
      throw(e);
    }
  }

  public RemoteIterator<FileStatus> listStatusIterator(final Path p)
        throws FileNotFoundException, IOException {
    AutoLock lock = sync.lockListing(p, Depth.DIRECTORY);
    try {
      RemoteIterator<FileStatus> iterator = fs.listStatusIterator(p);
      return new LockedRemoteIterator<FileStatus>(iterator, lock);
    } catch (Exception e) {
      lock.close();
      throw(e);
    }
  }

  public RemoteIterator<LocatedFileStatus> listFiles(
        final Path f, final boolean recursive)
        throws FileNotFoundException, IOException {
    Depth depth = recursive ? Depth.RECURSIVE : Depth.DIRECTORY;
    AutoLock lock = sync.lockListing(f, depth);
    try {
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(f, recursive);
      return new LockedRemoteIterator<LocatedFileStatus>(iterator, lock);
    } catch (Exception e) {
      lock.close();
      throw(e);
    }
  }

  public boolean mkdirs(Path f) throws IOException {
    // TODO this has implications for the parent dirs too
    try (AutoLock l = sync.lock(f)) {
      return fs.mkdirs(f);
    }
  }

  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    try (AutoLock l = sync.lock(f)) {
      return fs.mkdirs(f, permission);
    }
  }

  public void copyFromLocalFile(Path src, Path dst)
        throws IOException {
    try (AutoLock l = sync.lock(dst)) {
      fs.copyFromLocalFile(src, dst);
    }
  }

  public void moveFromLocalFile(Path[] srcs, Path dst)
        throws IOException {
    try (AutoLock l = sync.lock(dst)) {
      fs.moveFromLocalFile(srcs, dst);
    }
  }

  public void moveFromLocalFile(Path src, Path dst)
        throws IOException {
    try (AutoLock l = sync.lock(dst)) {
      fs.moveFromLocalFile(src, dst);
    }
  }

  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
        throws IOException {
    try (AutoLock l = sync.lock(dst)) {
      fs.copyFromLocalFile(delSrc, src, dst);
    }
  }

  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path[] srcs, Path dst)
        throws IOException {
    try (AutoLock l = sync.lock(dst)) {
      fs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    }
  }

  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path src, Path dst)
        throws IOException {
    try (AutoLock l = sync.lock(dst)) {
      fs.copyFromLocalFile(delSrc, overwrite, src, dst);
    }
  }

  public void copyToLocalFile(Path src, Path dst) throws IOException {
    try (AutoLock l = sync.lock(src)) {
      fs.copyToLocalFile(src, dst);
    }
  }

  public void moveToLocalFile(Path src, Path dst) throws IOException {
    try (AutoLock l = sync.lockDelete(src)) {
      fs.moveToLocalFile(src, dst);
    }
  }

  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
        throws IOException {
    try (AutoLock l = sync.lock(src)) {
      fs.copyToLocalFile(delSrc, src, dst);
    }
  }

  public void copyToLocalFile(boolean delSrc, Path src, Path dst,
                              boolean useRawLocalFileSystem) throws IOException {
    try (AutoLock l = sync.lock(src)) {
      fs.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
    }
  }

  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
        throws IOException {
    return fs.startLocalOutput(fsOutputFile, tmpLocalFile);
  }

  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
        throws IOException {
    try (AutoLock l = sync.lockWrite(fsOutputFile)) {
      fs.completeLocalOutput(fsOutputFile, tmpLocalFile);
    }
  }

  @Override
  public void close() throws IOException {
    // FS must close first so that FileSystem.processDeleteOnExit() can run
    // while locking is still available.
    fs.close();
    super.close();
    sync.close();
  }

  @Deprecated
  public long getBlockSize(Path f) throws IOException {
    try (AutoLock l = sync.lock(f)) {
      return fs.getBlockSize(f);
    }
  }

  public FileStatus getFileStatus(Path f) throws IOException {
    try (AutoLock l = sync.lock(f)) {
      return fs.getFileStatus(f);
    }
  }

  public void access(Path path, FsAction mode) throws AccessControlException,
        FileNotFoundException, IOException {
    try (AutoLock l = sync.lock(path)) {
      fs.access(path, mode);
    }
  }

  public void createSymlink(final Path target, final Path link,
                            final boolean createParent) throws AccessControlException,
        FileAlreadyExistsException, FileNotFoundException,
        ParentNotDirectoryException, UnsupportedFileSystemException,
        IOException {
    throw new UnsupportedOperationException("HBOSS does not support symlinks");
  }

  public FileStatus getFileLinkStatus(final Path f)
        throws AccessControlException, FileNotFoundException,
        UnsupportedFileSystemException, IOException {
    throw new UnsupportedOperationException("HBOSS does not support symlinks");
  }

  public boolean supportsSymlinks() {
    return false;
  }

  public Path getLinkTarget(Path f) throws IOException {
    throw new UnsupportedOperationException("HBOSS does not support symlinks");
  }

  public FileChecksum getFileChecksum(Path f) throws IOException {
    try (AutoLock l = sync.lock(f)) {
      return fs.getFileChecksum(f);
    }
  }

  public FileChecksum getFileChecksum(Path f, final long length)
        throws IOException {
    try (AutoLock l = sync.lock(f)) {
      return fs.getFileChecksum(f, length);
    }
  }

  public void setVerifyChecksum(boolean verifyChecksum) {
    fs.setVerifyChecksum(verifyChecksum);
  }

  public void setWriteChecksum(boolean writeChecksum) {
    fs.setWriteChecksum(writeChecksum);
  }

  public FsStatus getStatus() throws IOException {
    return fs.getStatus();
  }

  public FsStatus getStatus(Path p) throws IOException {
    try (AutoLock l = sync.lock(p)) {
      return fs.getStatus(p);
    }
  }

  public void setPermission(Path p, FsPermission permission
  ) throws IOException {
    try (AutoLock l = sync.lock(p)) {
      fs.setPermission(p, permission);
    }
  }

  public void setOwner(Path p, String username, String groupname
  ) throws IOException {
    try (AutoLock l = sync.lock(p)) {
      fs.setOwner(p, username, groupname);
    }
  }

  public void setTimes(Path p, long mtime, long atime
  ) throws IOException {
    try (AutoLock l = sync.lock(p)) {
      fs.setTimes(p, mtime, atime);
    }
  }

  public Path createSnapshot(Path path, String snapshotName)
        throws IOException {
    try (AutoLock l = sync.lockListing(path, Depth.RECURSIVE)) {
      return fs.createSnapshot(path, snapshotName);
    }
  }

  public void renameSnapshot(Path path, String snapshotOldName,
                             String snapshotNewName) throws IOException {
    fs.renameSnapshot(path, snapshotOldName, snapshotNewName);
  }

  public void deleteSnapshot(Path path, String snapshotName)
        throws IOException {
    fs.deleteSnapshot(path, snapshotName);
  }

  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
        throws IOException {
    try (AutoLock l = sync.lock(path)) {
      fs.modifyAclEntries(path, aclSpec);
    }
  }

  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
        throws IOException {
    try (AutoLock l = sync.lock(path)) {
      fs.removeAclEntries(path, aclSpec);
    }
  }

  public void removeDefaultAcl(Path path)
        throws IOException {
    try (AutoLock l = sync.lock(path)) {
      fs.removeDefaultAcl(path);
    }
  }

  public void removeAcl(Path path)
        throws IOException {
    try (AutoLock l = sync.lock(path)) {
      fs.removeAcl(path);
    }
  }

  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    try (AutoLock l = sync.lock(path)) {
      fs.setAcl(path, aclSpec);
    }
  }

  public AclStatus getAclStatus(Path path) throws IOException {
    try (AutoLock l = sync.lock(path)) {
      return fs.getAclStatus(path);
    }
  }

  public void setXAttr(Path path, String name, byte[] value)
        throws IOException {
    try (AutoLock l = sync.lock(path)) {
      fs.setXAttr(path, name, value);
    }
  }

  public void setXAttr(Path path, String name, byte[] value,
                       EnumSet<XAttrSetFlag> flag) throws IOException {
    try (AutoLock l = sync.lock(path)) {
      fs.setXAttr(path, name, value, flag);
    }
  }

  public byte[] getXAttr(Path path, String name) throws IOException {
    try (AutoLock l = sync.lock(path)) {
      return fs.getXAttr(path, name);
    }
  }

  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    try (AutoLock l = sync.lock(path)) {
      return fs.getXAttrs(path);
    }
  }

  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
        throws IOException {
    try (AutoLock l = sync.lock(path)) {
      return fs.getXAttrs(path, names);
    }
  }

  public List<String> listXAttrs(Path path) throws IOException {
    try (AutoLock l = sync.lock(path)) {
      return fs.listXAttrs(path);
    }
  }

  public void removeXAttr(Path path, String name) throws IOException {
    try (AutoLock l = sync.lock(path)) {
      fs.removeXAttr(path, name);
    }
  }
}
