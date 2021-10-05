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

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hbase.oss.TestUtils;
import org.junit.Assume;
import org.junit.Test;

public class TestHBOSSContractCreate extends AbstractContractCreateTest {

  @Override
  protected Configuration createConfiguration() {
    return super.createConfiguration();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new HBOSSContract(conf);
  }

  @Test
  @Override
  public void testCreatedFileIsVisibleOnFlush() throws Throwable {
    skipIfFilesNotVisibleDuringCreation();
    super.testCreatedFileIsVisibleOnFlush();
  }

  private void skipIfFilesNotVisibleDuringCreation() {
    Configuration conf = createConfiguration();
    try {
      TestUtils.getFileSystem(conf);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception configuring FS: " + e);
    }
    // HBOSS satisfies the contract that this test checks for, but it also
    // relies on flush, which s3a still does not support.
    Assume.assumeFalse(TestUtils.fsIs(TestUtils.S3A, conf));
  }

  @Test
  @Override
  public void testCreatedFileIsImmediatelyVisible() throws Throwable {
    describe("verify that a newly created file exists as soon as open returns");
    Path path = path("testCreatedFileIsImmediatelyVisible");
    try(FSDataOutputStream out = getFileSystem().create(path,
                                   false,
                                   4096,
                                   (short) 1,
                                   1024)) {

      // The original contract test delays close() until after the files appear.
      // HBOSS only guarantees that create() + close() is atomic, so the
      // original test still wouldn't work.
      out.close();

      if (!getFileSystem().exists(path)) {

        if (isSupported(CREATE_VISIBILITY_DELAYED)) {
          // For some file systems, downgrade to a skip so that the failure is
          // visible in test results.
          ContractTestUtils.skip(
                "This Filesystem delays visibility of newly created files");
        }
        assertPathExists("expected path to be visible before anything written",
                         path);
      }
    }
  }

  public void testSyncable() throws Throwable {
    skipIfFilesNotVisibleDuringCreation();
    // testSyncable() only exists in >=Hadoop-3.3.1. Selectively skip this test when
    // the method doesn't exist.
    try {
      Method testSyncable = AbstractContractCreateTest.class.getMethod("testSyncable");
      // super.testSyncable()
      testSyncable.invoke(this);
    } catch (NoSuchMethodException e) {
      Assume.assumeTrue("testSyncable does not exist on the parent, skipping test", false);
    }
  }
}
