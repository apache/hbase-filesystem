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
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;
import org.junit.Test;

public class TestHBOSSContractDistCp extends AbstractContractDistCpTest {

  @Override
  protected Configuration createConfiguration() {
    return super.createConfiguration();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new HBOSSContract(conf);
  }

  @Override
  @Test
  public void testDistCpWithIterator() {
    // We are currently ignoring this test case because the parent test case reads the captured log and makes
    // assertions on it. Since we've transitioned to log4j2, the log capturing in the test is not functioning properly.
    // Hadoop replaced log4j1.x with reload4j as part of HADOOP-18088 available only in Hadoop 3.3.3.
    // However, as hbase-filesystem uses Hadoop 3.3.2, we have decided to temporarily exclude this test case from execution.
  }
}
