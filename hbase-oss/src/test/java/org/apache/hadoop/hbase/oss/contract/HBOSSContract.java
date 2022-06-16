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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.Constants;
import org.apache.hadoop.hbase.oss.HBaseObjectStoreSemantics;
import org.apache.hadoop.hbase.oss.TestUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.oss.TestUtils.addContract;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HBOSSContract extends AbstractFSContract {
  private static final Logger LOG =
      LoggerFactory.getLogger(HBOSSContract.class);

  private Configuration conf = null;
  private HBaseObjectStoreSemantics fs = null;

  /**
   * Constructor: loads the authentication keys if found
   * @param conf configuration to work with
   */
  public HBOSSContract(Configuration conf) {
    super(conf);
    this.conf = conf;
    addContract(conf);
  }

  /**
   * Any initialisation logic can go here
   * @throws IOException IO problems
   */
  public void init() {

  }

  /**
   * Get the filesystem for these tests
   * @return the test fs
   * @throws IOException IO problems
   */
  public FileSystem getTestFileSystem() throws IOException {
    if (fs == null) {
      try {
        fs = TestUtils.getFileSystem(conf);
      } catch (Exception e) {
        LOG.error(e.toString(), e);
        throw new IOException("Failed to get FS", e);
      }
    }
    return fs;
  }

  /**
   * Get the scheme of this FS
   * @return the scheme this FS supports
   */
  public String getScheme() {
    return conf.get(Constants.CONTRACT_TEST_SCHEME, "s3a");
  }

  /**
   * Return the path string for tests, e.g. <code>file:///tmp</code>
   * @return a path in the test FS
   */
  public Path getTestPath() {
    return TestUtils.testPath(fs, "contract-tests");
  }

  @Override
  public String toString() {
    return "FSContract for HBOSS/" + getScheme();
  }
}
