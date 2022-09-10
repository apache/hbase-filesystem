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

import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.hbase.oss.TestUtils.addContract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HBaseObjectStoreSemanticsTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(HBaseObjectStoreSemanticsTest.class);

  protected HBaseObjectStoreSemantics hboss = null;
  protected TreeLockManager sync = null;

  public Path testPathRoot() {
    return TestUtils.testPathRoot(hboss);
  }

  public Path testPath(String path) {
    return TestUtils.testPath(hboss, path);
  }

  public TreeLockManager getLockManager() {
    return sync;
  }

  @Before
  public void setup() throws Exception {
    Configuration conf = createConfiguration();
    addContract(conf);
    hboss = TestUtils.getFileSystem(conf);
    sync = hboss.getLockManager();
    hboss.mkdirs(testPathRoot());
  }

  /**
   * Create the configuration for the test FS.
   * @return a configuration.
   */
  protected Configuration createConfiguration() {
    return new Configuration();
  }

  @After
  public void tearDown() throws Exception {
    if (hboss != null) {
      LOG.info("Store statistics {}",
          ioStatisticsToPrettyString(hboss.getIOStatistics()));
    }
    TestUtils.cleanup(hboss);
  }
}
