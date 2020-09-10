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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Bypasses all synchronization to effectively make HBOSS operations no-ops.
 *
 * Can be enabled in JUnit tests with -Pnull to reproduce the problems HBOSS is
 * intended to fix.
 */
public class NullTreeLockManager extends TreeLockManager {

  public void initialize(FileSystem fs) {
    this.fs = fs;
  }

  @Override
  protected void writeLock(Path p) {
  }

  @Override
  protected void writeUnlock(Path p) {
  }

  @Override
  protected void readLock(Path p) {
  }

  @Override
  protected void readUnlock(Path p) {
  }

  @Override
  protected boolean writeLockAbove(Path p) {
    return false;
  }

  @Override
  @VisibleForTesting
  public boolean writeLockBelow(Path p, Depth depth) {
    return false;
  }

  @Override
  @VisibleForTesting
  public boolean readLockBelow(Path p, Depth depth) {
    return false;
  }

  @Override
  protected void recursiveDelete(Path p) {
  }
}

