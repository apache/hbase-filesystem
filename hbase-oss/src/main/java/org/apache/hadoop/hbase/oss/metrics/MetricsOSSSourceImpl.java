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

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop2 implementation of MetricsOSSSource.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
@InterfaceAudience.Private
public class MetricsOSSSourceImpl
    extends BaseSourceImpl implements MetricsOSSSource {

  private static MetricsOSSSourceImpl instance = null;

  private final MetricHistogram acquireRenameLockHisto;
  private final MetricHistogram releaseRenameLockHisto;
  private final MetricHistogram renameFsOperationHisto;


  private MetricsOSSSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  private MetricsOSSSourceImpl(String metricsName,
                                       String metricsDescription,
                                       String metricsContext,
                                       String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    acquireRenameLockHisto = getMetricsRegistry().newTimeHistogram(ACQUIRE_RENAME_LOCK, ACQUIRE_RENAME_LOCK_DESC);
    releaseRenameLockHisto = getMetricsRegistry().newTimeHistogram(RELEASE_RENAME_LOCK, RELEASE_RENAME_LOCK_DESC);
    renameFsOperationHisto = getMetricsRegistry().newTimeHistogram(RENAME_FS_OPERATION, RENAME_FS_OPERATION_DESC);

  }

  @Override
  public void updateAcquireRenameLockHisto(long t) {
    acquireRenameLockHisto.add(t);
  }

  @Override
  public void updateReleaseRenameLockHisto(long t) {
    releaseRenameLockHisto.add(t);
  }

  @Override
  public void updateRenameFsOperationHisto(long t) {
    renameFsOperationHisto.add(t);
  }


  /**
   * Yes this is a get function that doesn't return anything.  Thanks Hadoop for breaking all
   * expectations of java programmers.  Instead of returning anything Hadoop metrics expects
   * getMetrics to push the metrics into the collector.
   *
   * @param metricsCollector Collector to accept metrics
   * @param all              push all or only changed?
   */
  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder mrb = metricsCollector.addRecord(metricsName);

    metricsRegistry.snapshot(mrb, all);

    // source is registered in supers constructor, sometimes called before the whole initialization.
    if (metricsAdapter != null) {
      // snapshot MetricRegistry as well
      metricsAdapter.snapshotAllMetrics(registry, mrb);
    }
  }


  public static synchronized MetricsOSSSourceImpl getInstance(){
    if(instance == null){
      instance = new MetricsOSSSourceImpl();
    }
    return instance;
  }
}
