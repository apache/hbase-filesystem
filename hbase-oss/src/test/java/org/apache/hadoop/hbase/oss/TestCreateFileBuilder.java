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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.sync.AutoLock;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * test the createFile() builder API, where the existence checks
 * take place in build(), not the FS API call.
 * This means the lock checking needs to be postponed.
 */
public class TestCreateFileBuilder extends HBaseObjectStoreSemanticsTest {
  private static final Logger LOG =
        LoggerFactory.getLogger(TestCreateFileBuilder.class);
  public static final int SLEEP_INTERVAL = 5_000;

  @Test
  public void testCreateOverlappingBuilders() throws Exception {
    Path path = testPath("testCreateOverlappingBuilders");
    ExecutorService svc = Executors.newSingleThreadExecutor();

    FSDataOutputStream stream = null;
    try {
      FSDataOutputStreamBuilder builder = hboss.createFile(path)
          .overwrite(false);
      // build the second of these.
      // even before the stream is closed, the first builder's build
      // call must fail.
      LOG.info("building {}:", builder);
      stream = builder.build();

      Assertions.assertThat(stream)
          .describedAs("expected a LockedFSDataOutputStream")
          .isInstanceOf(AutoLock.LockedFSDataOutputStream.class);

      LOG.info("Output stream  {}:", stream);
      stream.write(0);


      // submit the writer into a new thread


      Future<Long> fut = svc.submit(() -> {
        try {
          FSDataOutputStreamBuilder builder1 = hboss.createFile(path)
              .overwrite(true);
          LOG.info("Inner thread building {}", builder1);
          try (FSDataOutputStream out = builder1.build()) {
            long executionTime = System.currentTimeMillis();
            LOG.info("Inner thread file created at {}", executionTime);
            out.write(Bytes.toBytes("localhost"));
            out.flush();
            return executionTime;
          }
        } catch (Exception e) {
          LOG.error("Caught exception", e);
          throw new AssertionError("Failed to create file", e);
        }
      });

      LOG.info("main thread sleeping");
      Thread.sleep(SLEEP_INTERVAL);
      long streamCloseTimestamp = System.currentTimeMillis();
      LOG.info("main thread closing stream");
      stream.close();
      // try twice for rigorousness
      stream.close();
      long threadTimestamp = fut.get(15, TimeUnit.SECONDS);
      Assertions.assertThat(threadTimestamp)
          .describedAs("timestamp of file creation in the second thread")
          .isGreaterThanOrEqualTo(streamCloseTimestamp);

    } finally {
      if (stream != null) {
        stream.close();
      }
      hboss.delete(path, false);
    }
  }

}
