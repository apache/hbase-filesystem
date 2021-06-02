package org.apache.hadoop.hbase.oss.metrics;

import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.junit.Assert;
import org.junit.Test;

public class TestMetricsOSSSource {

  @Test
  public void testUpdateValues(){
    MetricsOSSSourceImpl metrics = MetricsOSSSourceImpl.getInstance();
    metrics.updateAcquireRenameLockHisto(1l);
    metrics.updateRenameFsOperationHisto(2l);
    metrics.updateReleaseRenameLockHisto(3l);

    Assert.assertEquals(1l, ((MutableHistogram) metrics.getMetricsRegistry()
        .getHistogram(MetricsOSSSource.ACQUIRE_RENAME_LOCK)).getMax());
    Assert.assertEquals(2l, ((MutableHistogram) metrics.getMetricsRegistry()
        .getHistogram(MetricsOSSSource.RENAME_FS_OPERATION)).getMax());
    Assert.assertEquals(3l, ((MutableHistogram) metrics.getMetricsRegistry()
        .getHistogram(MetricsOSSSource.RELEASE_RENAME_LOCK)).getMax());
  }
}
