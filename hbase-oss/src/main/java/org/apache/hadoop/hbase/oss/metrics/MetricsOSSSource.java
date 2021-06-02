/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.oss.metrics;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface for classes that expose metrics about the Object Store.
 */
@InterfaceAudience.Private
public interface MetricsOSSSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "FileSystem";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "objectstore";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about Object Store";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "ObjectStore,sub=" + METRICS_NAME;

  /**
   * Update the Rename AcquireLock Histogram
   *
   * @param t time it took
   */
  public void updateAcquireRenameLockHisto(long t);

  /**
   * Update the Rename ReleaseLock Histogram
   *
   * @param t time it took
   */
  public void updateReleaseRenameLockHisto(long t);

  /**
   * Update the Rename FsOperation Histogram
   *
   * @param t time it took
   */
  public void updateRenameFsOperationHisto(long t);

  String ACQUIRE_RENAME_LOCK = "acquireRenameLock";
  String ACQUIRE_RENAME_LOCK_DESC = "Time in ms required to acquire lock for Rename";
  String RELEASE_RENAME_LOCK = "releaseRenameLock";
  String RELEASE_RENAME_LOCK_DESC = "Time in ms required to release lock for Rename";
  String RENAME_FS_OPERATION = "renameFsOperation";
  String RENAME_FS_OPERATION_DESC = "Time in ms required to finish Rename file operation";

}
