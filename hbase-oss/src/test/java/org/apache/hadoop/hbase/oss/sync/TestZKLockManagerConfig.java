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
package org.apache.hadoop.hbase.oss.sync;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.oss.Constants;
import org.junit.Test;

public class TestZKLockManagerConfig {

  @Test
  public void testLockManagerConfigFallback() throws Exception {
    EmbeddedZK zk1 = new EmbeddedZK();
    ZKTreeLockManager lockMgr = null;
    Configuration conf = new Configuration(false);
    conf.setClass(Constants.SYNC_IMPL, ZKTreeLockManager.class, TreeLockManager.class);
    try {
      zk1.conditionalStart(conf);
  
      String expectedQuorum = zk1.getConnectionString();
      assertNotNull(expectedQuorum);

      // Validate that both config properties are set
      assertEquals(conf.get(Constants.ZK_CONN_STRING), expectedQuorum);
      assertEquals(conf.get(HConstants.ZOOKEEPER_QUORUM), expectedQuorum);

      // Unset the HBoss-specific property to force the LockManager to use the HBase ZK quorum
      conf.unset(Constants.ZK_CONN_STRING);
      assertNull(conf.get(Constants.ZK_CONN_STRING));
      
      // Create a LocalFS -- we don't really care about it, just passing it to the lockManager.
      // why a new instance? to avoid problems with cached fs instances across tests.
      FileSystem fs = new LocalFileSystem();
      fs.initialize(URI.create("file:///"), conf);

      // Initializing the ZKTreeLockManager should succeed, even when we only have
      // the hbase.zookeeper.quorum config property set.
      lockMgr = new ZKTreeLockManager();
      lockMgr.initialize(fs);
    } finally {
      // Clean up everything.
      if (lockMgr != null) {
        lockMgr.close();
      }
      zk1.conditionalStop();
    }
  }
}
