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

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.hbase.oss.TestUtils;


/**
 * For S3A-specific extension of AbstractContractOpenTest, we needed to override
 * areZeroByteFilesEncrypted() method to always return true.
 * TestHBOSSContractOpen remains to be run in the general case.
 */
public class TestHBOSSContractOpenS3A extends TestHBOSSContractOpen {

  @Override
  protected Configuration createConfiguration() {
    return super.createConfiguration();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    AbstractFSContract contract = new HBOSSContract(conf);
    TestUtils.runIfS3(true, conf);
    return contract;
  }

  /**
   * S3A always declares zero byte files as encrypted.
   * @return true, always.
   */
  protected boolean areZeroByteFilesEncrypted() {
    return true;
  }

  /**
   * HADOOP-16202 cut this test as it is now OK to
   * pass down a null status.
   */
  @Test
  public void testOpenFileNullStatus() {

  }
}
