/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.oss;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.oss.sync.AutoLock;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager;
import org.apache.hadoop.util.Progressable;

/**
 * A locked output stream builder for createFile().
 * The lock is acquired in the {@link #build()} call
 * and kept in the output stream where it is held
 * until closed.
 * As the lock is by thread, the same file can be opened
 * more than once by that same thread.
 *
 * Not all the getter methods in the wrapped builder
 * are accessible, so to make them retrievable for
 * testing, the superclass version of them are called
 * as well as the wrapped class being involed.
 */
@SuppressWarnings("rawtypes")
class LockedCreateFileBuilder
    extends FSDataOutputStreamBuilder<FSDataOutputStream, LockedCreateFileBuilder> {
  private static final Logger LOG =
      LoggerFactory.getLogger(LockedCreateFileBuilder.class);

  /**
   * Path of the file.
   */
  private final Path path;

  /**
   * Lock manager.
   */
  private final TreeLockManager sync;

  private final FSDataOutputStreamBuilder wrapped;

  /**
   * Constructor adding locking to a builder.
   * @param fileSystem FS
   * @param path path
   * @param sync sync point
   * @param wrapped wrapped stream builder.
   */
  LockedCreateFileBuilder(
      @Nonnull final FileSystem fileSystem,
      final Path path,
      final TreeLockManager sync,
      final FSDataOutputStreamBuilder wrapped) {
    super(fileSystem, path);
    this.path = path;
    this.sync = sync;
    this.wrapped = wrapped;
  }

  @Override
  public FSDataOutputStream build() throws IOException {
    LOG.debug("Building output stream for {}", path);
    AutoLock lock = sync.lockWrite(path);
    try {
      FSDataOutputStream stream = getWrapped().build();
      return new AutoLock.LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw e;
    }
  }

  @Override
  public String toString() {
    return "LockedCreateFileBuilder{" +
        "path=" + path +
        "} " + super.toString();
  }



  @Override
  public LockedCreateFileBuilder overwrite(final boolean overwrite) {
    super.overwrite(overwrite);
    getWrapped().overwrite(overwrite);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder permission(@Nonnull final FsPermission perm) {
    super.permission(perm);
    getWrapped().permission(perm);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder bufferSize(final int bufSize) {
    super.bufferSize(bufSize);
    getWrapped().bufferSize(bufSize);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder replication(final short replica) {
    getWrapped().replication(replica);
    return getThisBuilder();
  }


  @Override
  public LockedCreateFileBuilder blockSize(final long blkSize) {
    super.blockSize(blkSize);
    getWrapped().blockSize(blkSize);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder recursive() {
    super.recursive();
    getWrapped().recursive();
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder progress(@Nonnull final Progressable prog) {
    super.progress(prog);
    getWrapped().progress(prog);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder checksumOpt(
      @Nonnull final Options.ChecksumOpt chksumOpt) {
    super.checksumOpt(chksumOpt);
    getWrapped().checksumOpt(chksumOpt);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder getThisBuilder() {
    return this;
  }

  @Override
  public LockedCreateFileBuilder opt(@Nonnull final String key,
      @Nonnull final String value) {
    getWrapped().opt(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder opt(@Nonnull final String key, final boolean value) {
    getWrapped().opt(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder opt(@Nonnull final String key, final int value) {
    getWrapped().opt(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder opt(@Nonnull final String key, final float value) {
    getWrapped().opt(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder opt(@Nonnull final String key, final double value) {
    getWrapped().opt(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder opt(@Nonnull final String key,
      @Nonnull final String... values) {
    getWrapped().opt(key, values);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder must(@Nonnull final String key,
      @Nonnull final String value) {
    getWrapped().must(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder must(@Nonnull final String key, final boolean value) {
    getWrapped().must(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder must(@Nonnull final String key, final int value) {
    getWrapped().must(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder must(@Nonnull final String key, final float value) {
    getWrapped().must(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder must(@Nonnull final String key, final double value) {
    getWrapped().must(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder must(@Nonnull final String key,
      @Nonnull final String... values) {
    getWrapped().must(key, values);
    return getThisBuilder();
  }

  /**
   * Configure with a long value.
   *  was not on the original interface.
   * It is implemented in the wrapper by converting
   * to a string and calling the wrapper's
   * {@code #opt(String, String)}.
   */
  public LockedCreateFileBuilder opt(@Nonnull String key, long value) {
    return opt(key, Long.toString(value));
  }

  /**
   * Configure with a long value.
   * must(String, Long) was not on the original interface.
   * It is implemented in the wrapper by converting
   * to a string and calling the wrapper's
   * {@code #must(String, String)}.
   */
  public LockedCreateFileBuilder must(@Nonnull String key, long value) {
    return must(key, Long.toString(value));
  }

  @Override
  public Configuration getOptions() {
    return getWrapped().getOptions();
  }

  @Override
  public Set<String> getMandatoryKeys() {
    return getWrapped().getMandatoryKeys();
  }

  /**
   * Can't call into the wrapped class, so reimplement.
   * {@inheritDoc}
   */
  @Override
  public void rejectUnknownMandatoryKeys(final Collection<String> knownKeys,
      final String extraErrorText)
      throws IllegalArgumentException {
    rejectUnknownMandatoryKeys(getMandatoryKeys(),
        knownKeys, extraErrorText);
  }

  /**
   * The wrapped builder.
   */
  public FSDataOutputStreamBuilder getWrapped() {
    return wrapped;
  }

  @Override
  public LockedCreateFileBuilder create() {
    super.create();
    getWrapped().create();
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder append() {
    super.create();
    getWrapped().append();
    return getThisBuilder();
  }

  @Override
  public Progressable getProgress() {
    return super.getProgress();
  }
}
