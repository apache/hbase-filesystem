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

package org.apache.hadoop.hbase.oss.sync;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;

/**
 * An input stream builder which locks the path to read on the build() call.
 * all builder operations must return this instance so that the final
 * build will acquire the lock.
 */
public class LockedFutureDataInputStreamBuilder implements FutureDataInputStreamBuilder {
  private final TreeLockManager sync;
  private final Path path;
  private final FutureDataInputStreamBuilder wrapped;

  public LockedFutureDataInputStreamBuilder(final TreeLockManager sync,
      final Path path,
      final FutureDataInputStreamBuilder wrapped) {
    this.sync = sync;
    this.path = path;
    this.wrapped = wrapped;
  }

  @Override
  public CompletableFuture<FSDataInputStream> build()
      throws IllegalArgumentException, UnsupportedOperationException, IOException {
    try (AutoLock l = sync.lock(path)) {
      return wrapped.build();
    }
  }

  @Override
  public FutureDataInputStreamBuilder opt(@Nonnull final String key,
      @Nonnull final String value) {
    wrapped.opt(key, value);
    return this;
  }

  @Override
  public FutureDataInputStreamBuilder opt(@Nonnull final String key, final boolean value) {
    wrapped.opt(key, value);
    return this;
  }

  @Override
  public FutureDataInputStreamBuilder opt(@Nonnull final String key, final int value) {
    wrapped.opt(key, value);
    return this;
  }

  @Override
  public FutureDataInputStreamBuilder opt(@Nonnull final String key, final float value) {
    wrapped.opt(key, value);
    return this;
  }

  @Override
  public FutureDataInputStreamBuilder opt(@Nonnull final String key, final double value) {
    wrapped.opt(key, value);
    return this;
  }

  @Override
  public FutureDataInputStreamBuilder opt(@Nonnull final String key,
      @Nonnull final String... values) {
    wrapped.opt(key, values);
    return this;
  }

  @Override
  public FutureDataInputStreamBuilder must(@Nonnull final String key,
      @Nonnull final String value) {
    wrapped.must(key, value);
    return this;
  }

  @Override
  public FutureDataInputStreamBuilder must(@Nonnull final String key, final boolean value) {
    wrapped.must(key, value);
    return this;
  }

  @Override
  public FutureDataInputStreamBuilder must(@Nonnull final String key, final int value) {
    wrapped.must(key, value);
    return this;
  }

  @Override
  public FutureDataInputStreamBuilder must(@Nonnull final String key, final float value) {
    wrapped.must(key, value);
    return this;
  }

  @Override
  public FutureDataInputStreamBuilder must(@Nonnull final String key, final double value) {
    wrapped.must(key, value);
    return this;
  }

  @Override
  public FutureDataInputStreamBuilder must(@Nonnull final String key,
      @Nonnull final String... values) {
    wrapped.must(key, values);
    return this;
  }

  @Override
  public FutureDataInputStreamBuilder withFileStatus(final FileStatus status) {
    wrapped.withFileStatus(status);
    return this;
  }

  /**
   * Configure with a long value.
   * This is not on the original interface.
   * It is implemented in the wrapper by converting
   * to a string and calling the wrapper's
   * {@code #opt(String, String)}.
   */
  public FutureDataInputStreamBuilder opt(@Nonnull String key, long value) {
    wrapped.opt(key, Long.toString(value));
    return this;
  }

  /**
   * Configure with a long value.
   * This is not on the original interface.
   * It is implemented in the wrapper by converting
   * to a string and calling the wrapper's
   * {@code #must(String, String)}.
   */
  public FutureDataInputStreamBuilder must(@Nonnull String key, long value) {
    wrapped.must(key, Long.toString(value));
    return this;
  }

}
