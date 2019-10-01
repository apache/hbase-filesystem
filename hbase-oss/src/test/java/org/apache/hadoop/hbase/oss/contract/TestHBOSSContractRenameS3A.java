/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hbase.oss.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hbase.oss.TestUtils;

/**
 * There is an S3A-specific extension of AbstractContractRenameTest, and this
 * class implements the same modifications for HBOSS-on-S3A.
 * TestHBOSSContractRename remains to be run in the general case.
 */
public class TestHBOSSContractRenameS3A extends AbstractContractRenameTest {

  @Override
  protected Configuration createConfiguration() {
    return super.createConfiguration();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    HBOSSContract contract = new HBOSSContract(conf);
    TestUtils.runIfS3(true, conf);
    return contract;
  }

  // This is copied from Hadoop's ITestS3AContractRename. Ideally we could
  // extend that instead of AbstractContractRenameTest, but it is not published.
  @Override
  public void testRenameDirIntoExistingDir() throws Throwable {
    describe("Verify renaming a dir into an existing dir puts the files"
             +" from the source dir into the existing dir"
             +" and leaves existing files alone");
    FileSystem fs = getFileSystem();
    String sourceSubdir = "source";
    Path srcDir = path(sourceSubdir);
    Path srcFilePath = new Path(srcDir, "source-256.txt");
    byte[] srcDataset = ContractTestUtils.dataset(256, 'a', 'z');
    ContractTestUtils.writeDataset(fs, srcFilePath, srcDataset, srcDataset.length, 1024, false);
    Path destDir = path("dest");

    Path destFilePath = new Path(destDir, "dest-512.txt");
    byte[] destDateset = ContractTestUtils.dataset(512, 'A', 'Z');
    ContractTestUtils.writeDataset(fs, destFilePath, destDateset, destDateset.length, 1024,
        false);
    assertIsFile(destFilePath);

    boolean rename = fs.rename(srcDir, destDir);
    assertFalse("s3a doesn't support rename to non-empty directory", rename);
  }
}
