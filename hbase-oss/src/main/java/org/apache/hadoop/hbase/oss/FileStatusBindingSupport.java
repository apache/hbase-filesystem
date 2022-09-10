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

import java.util.function.BiFunction;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Class to make sure that the right filestatus
 * types for s3a are passed down *or not set at all*.
 * This is because hadoop 3.3.0 was fussy about the type of
 * the status.
 */
public class FileStatusBindingSupport {

  public static final String S3AFS = "org.apache.hadoop.fs.s3a.S3AFileSystem";
  private final boolean isS3A;
  private final BiFunction<Path, FileStatus, Boolean> propagateStatusProbe;

  /**
   * Create for a target filesystem.
   * @param fs filesystem
   */
  public FileStatusBindingSupport(FileSystem fs) {
    this(fs.getClass().getName());
  }

  public FileStatusBindingSupport(String name) {
    isS3A = name.equals(S3AFS);
    propagateStatusProbe = isS3A
        ? FileStatusBindingSupport::allowOnlyS3A
        : FileStatusBindingSupport::allowAll;
  }

  /**
   * Is the FS s3a?
   *
   * @return true if the fs classname is that of s3afs.
   */
  public boolean isS3A() {
    return isS3A;
  }

  /**
   * Get the status probe of the fs.
   *
   * @return the status probe
   */
  public BiFunction<Path, FileStatus, Boolean> getPropagateStatusProbe() {
    return propagateStatusProbe;
  }

  /**
   * A status is allowed if non-null and its path matches that of the file
   * to open.
   *
   * @param p path
   * @param st status
   *
   * @return true if the status should be passed down.
   */
  public static boolean allowAll(Path p, FileStatus st) {
    return st != null && p.equals(st.getPath());
  }

  /**
   * A status is allowed if it meets the criteria of
   * {@link #allowAll(Path, FileStatus)} and the
   * status is an S3AFileStatus.
   *
   * @param p path
   * @param st status
   *
   * @return true if the status should be passed down.
   */
  public static boolean allowOnlyS3A(Path p, FileStatus st) {
    return allowAll(p, st)
        && st.getClass().getName().equals("org.apache.hadoop.fs.s3a.S3AFileStatus");
  }

}
