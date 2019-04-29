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

import java.net.InetAddress;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.oss.Constants;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager;
import org.apache.hadoop.hbase.oss.sync.ZKTreeLockManager;

public class EmbeddedZK {

  private static HBaseZKTestingUtility util = null;

  public static synchronized void conditionalStart(Configuration conf) throws Exception {
    Class implementation = conf.getClass(Constants.SYNC_IMPL, TreeLockManager.class);
    boolean notConfigured = StringUtils.isEmpty(conf.get(Constants.ZK_CONN_STRING));
    if (implementation == ZKTreeLockManager.class && notConfigured) {
      if (util == null) {
        util = new HBaseZKTestingUtility(conf);
        util.startMiniZKCluster();
      }
      int port = util.getZkCluster().getClientPort();
      String hostname = InetAddress.getLocalHost().getHostName();
      String connectionString = hostname + ":" + port;
      conf.set(Constants.ZK_CONN_STRING, connectionString);
    }
  }

  public static synchronized void conditionalStop() throws Exception {
    if (util != null) {
      util.shutdownMiniZKCluster();
      util = null;
    }
  }
}
