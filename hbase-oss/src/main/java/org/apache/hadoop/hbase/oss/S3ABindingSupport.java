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

import java.util.function.Function;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

/**
 * Class to make sure that the right filestatus
 * types for s3a are passed down *or not set at all*.
 */
public class S3ABindingSupport {

  private final boolean isS3A;
  private final Function<FileStatus, Boolean> propagateStatusProbe;

  public S3ABindingSupport(FileSystem fs) {
    this(fs.getClass().getName());
  }

  public S3ABindingSupport(String name) {
    isS3A = name
        .equals("org.apache.hadoop.fs.s3a.S3AFileSystem");
    propagateStatusProbe = isS3A
        ? S3ABindingSupport::allowOnlyS3A
        : S3ABindingSupport::allowAll;
  }

  /**
   * Is the FS s3a?
   * @return true if the fs classname is that of s3afs.
   */
  public boolean isS3A() {
    return isS3A;
  }

  /**
   * Get the status probe of the fs.
   * @return the status probe
   */
  public Function<FileStatus, Boolean> getPropagateStatusProbe() {
    return propagateStatusProbe;
  }

  public static boolean allowAll(FileStatus st) {
    return st != null;
  }

  public static boolean allowOnlyS3A(FileStatus st) {
    return st != null &&
        st.getClass().getName().equals("org.apache.hadoop.fs.s3a.S3AFileStatus");
  }

}
