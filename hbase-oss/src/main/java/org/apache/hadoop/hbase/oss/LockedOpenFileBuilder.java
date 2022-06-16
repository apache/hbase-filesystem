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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager;

/**
 * An input stream builder which locks the path to read on the build() call.
 * all builder operations must return this instance so that the final
 * build will acquire the lock.
 *
 * There's a bit of extra complexity around the file status
 * as s3a in 3.3.0 overreacts to the passed in status not being
 * S3AFileStatus by raising an exception.
 * Later versions just ignore the value in the build() phase if it isn't.
 * A predicate can be passed in to determine if the status
 * should be added.
 */
public class LockedOpenFileBuilder extends
    LockingFSBuilderWrapper<CompletableFuture<FSDataInputStream>, FutureDataInputStreamBuilder>
    implements FutureDataInputStreamBuilder {

  private static final Logger LOG =
        LoggerFactory.getLogger(LockedOpenFileBuilder.class);

  private final Function<FileStatus, Boolean> propagateStatusProbe;
  public LockedOpenFileBuilder(@Nonnull final Path path,
      final TreeLockManager sync,
      final FutureDataInputStreamBuilder wrapped,
      final Function<FileStatus, Boolean> propagateStatusProbe) {
    super(path, sync, wrapped,
        LockedOpenFileBuilder::complete);
    this.propagateStatusProbe = propagateStatusProbe;
  }

  /**
   * Update the file status if the status probe is happy.
   * @param status status.
   * @return the builder.
   */
  public FutureDataInputStreamBuilder withFileStatus(
      final FileStatus status) {
    if (propagateStatusProbe.apply(status)) {
      getWrapped().withFileStatus(status);
    }

    return getThisBuilder();
  }

  /**
   * The completion operation simply logs the value at debug.
   * The open may include an async HEAD call, or skip the probe
   * entirely.
   * @param in input
   * @return the input
   */
  private static CompletableFuture<FSDataInputStream> complete(CompletableFuture<FSDataInputStream> in) {
    LOG.debug("Created input stream {}", in);
    return in;
  }

}
