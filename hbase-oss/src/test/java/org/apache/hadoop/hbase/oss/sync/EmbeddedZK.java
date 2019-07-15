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

import java.net.InetAddress;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.oss.Constants;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager;
import org.apache.hadoop.hbase.oss.sync.ZKTreeLockManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class EmbeddedZK {

  // Guards `testUtil` -- making sure we don't start mulitple ZKs.
  private final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();
  private Object testUtil = null;
  private String connectionString = null;

  public void conditionalStart(Configuration conf) throws Exception {
    Class<?> implementation = conf.getClass(Constants.SYNC_IMPL, TreeLockManager.class);
    if (implementation != ZKTreeLockManager.class) {
      return;
    }
    LOCK.readLock().lock();
    if (testUtil == null) {
      LOCK.readLock().unlock();
      try {
        LOCK.writeLock().lock();
        // If the server is non-null after we took the lock, someone else just beat
        // us here. Bail out.
        if (testUtil != null) {
          return;
        }
        
        start(conf);
      } finally {
        LOCK.writeLock().unlock();
      }
    } else {
      // Set the ZK connection details into this conf. It might not have them.
      setConfiguration(conf);
      LOCK.readLock().unlock();
    }
  }

  /**
   * Requires external synchronization!
   */
  protected void start(Configuration conf) throws Exception {
    Class<?> testUtilImpl;
    try {
      testUtilImpl = Class.forName("org.apache.hadoop.hbase.HBaseZKTestingUtility");
    } catch (ClassNotFoundException ex) {
      testUtilImpl = Class.forName("org.apache.hadoop.hbase.HBaseTestingUtility");
    }
    testUtil = testUtilImpl.getDeclaredConstructor(Configuration.class).newInstance(conf);
    testUtil.getClass().getDeclaredMethod("startMiniZKCluster").invoke(testUtil);

    Object zkCluster = testUtil.getClass().getDeclaredMethod("getZkCluster").invoke(testUtil);
    int port = (int) zkCluster.getClass().getDeclaredMethod("getClientPort").invoke(zkCluster);
    String hostname = InetAddress.getLocalHost().getHostName();
    connectionString = hostname + ":" + port;
    setConfiguration(conf);
  }

  public void conditionalStop() throws Exception {
    try {
      LOCK.writeLock().lock();
      if (testUtil != null) {
        testUtil.getClass().getDeclaredMethod("shutdownMiniZKCluster").invoke(testUtil);
        testUtil = null;
      }
    } finally {
      LOCK.writeLock().unlock();
    }
  }

  protected void setConfiguration(Configuration conf) {
    conf.set(Constants.ZK_CONN_STRING, connectionString);
    conf.set(HConstants.ZOOKEEPER_QUORUM, connectionString);
  }

  protected String getConnectionString() {
    try {
      LOCK.readLock().lock();
      return connectionString;
    } finally {
      LOCK.readLock().unlock();
    }
  }
}
