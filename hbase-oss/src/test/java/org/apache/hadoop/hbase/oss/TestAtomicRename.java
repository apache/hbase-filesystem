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

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAtomicRename extends HBaseObjectStoreSemanticsTest {
  public static final Logger LOG =
        LoggerFactory.getLogger(TestAtomicRename.class);

  @Test
  public void testAtomicRename() throws Exception {
    Path renameSource = testPath("atomicRenameSource");
    Path renameTarget = testPath("atomicRenameTarget");
    try {
      for (int i = 1; i <= 8; i++) {
        Path dir = new Path(renameSource, "dir" + i);
        hboss.mkdirs(dir);
        for (int j = 1; j <= 8; j++) {
          Path file = new Path(dir, "file" + j);
          FSDataOutputStream out = hboss.create(file);
          // Write 4kb
          for (int k = 0; k < 256; k++) {
            out.write("0123456789ABCDEF".getBytes());
          }
          out.close();
        }
      }

      Thread renameThread = new Thread(
        new Runnable() {
          public void run() {
            try {
              boolean success = hboss.rename(renameSource, renameTarget);
              Assert.assertTrue("Rename returned false, indicating some error.",
                    success);
            } catch(IOException e) {
              Assert.fail("Unexpected exception during rename: " + e);
            }
          }
        }
      );
      renameThread.start();

      // If the rename fails before ever creating the target, this will hang forever
      while (!hboss.exists(renameTarget)) {
        Thread.sleep(1);
      }
      Assert.assertFalse("Rename source is still visible after rename finished or target showed up.",
            hboss.exists(renameSource));
      renameThread.join();
    } finally {
      hboss.delete(renameSource);
      hboss.delete(renameTarget);
    }
  }


}
