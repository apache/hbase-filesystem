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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Constants  {
  public static final String DATA_URI = "fs.hboss.data.uri";
  public static final String SYNC_IMPL = "fs.hboss.sync.impl";

  public static final String ZK_CONN_STRING = "fs.hboss.sync.zk.connectionString";
  public static final String ZK_BASE_SLEEP_MS = "fs.hboss.sync.zk.sleep.base.ms";
  public static final String ZK_MAX_RETRIES = "fs.hboss.sync.zk.sleep.max.retries";

  public static final String CONTRACT_TEST_SCHEME = "fs.contract.test.fs.scheme";
}
