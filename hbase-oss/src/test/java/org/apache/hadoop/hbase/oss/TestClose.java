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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hbase.oss.sync.NullTreeLockManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestClose {

  @Test
  public void testCloseRemovesInstanceFromFileSystemCache() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "testFS://test");
    conf.set("fs.testFS.impl", HBaseObjectStoreSemantics.class.getName());
    conf.set("fs.hboss.fs.testFS.impl", LocalFileSystem.class.getName());
    conf.set(Constants.SYNC_IMPL, NullTreeLockManager.class.getName());
    FileSystem fileSystem = FileSystem.get(conf);
    fileSystem.close();
    Assert.assertFalse(fileSystem == FileSystem.get(conf));
  }
}
