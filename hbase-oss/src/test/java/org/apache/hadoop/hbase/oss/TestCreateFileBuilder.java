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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.sync.AutoLock;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * test the createFile() builder API, where the existence checks
 * take place in build(), not the FS API call.
 * This means the lock checking needs to be postponed.
 */
public class TestCreateFileBuilder extends HBaseObjectStoreSemanticsTest {
  private static final Logger LOG =
        LoggerFactory.getLogger(TestCreateFileBuilder.class);

  @Test
  public void testCreateOverlappingBuilders() throws Exception {
    Path path = testPath("testCreateOverlappingBuilders");

    FSDataOutputStream stream2 = null;
    try {
      FSDataOutputStreamBuilder builder1 = hboss.createFile(path)
          .overwrite(false);
      FSDataOutputStreamBuilder builder2 = hboss.createFile(path)
          .overwrite(false);
      // build the second of these.
      // even before the stream is closed, the first builder's build
      // call must fail.
      LOG.info("building {}:", builder2);
      stream2 = builder2.build();

      Assertions.assertThat(stream2)
          .describedAs("expected a LockedFSDataOutputStream")
          .isInstanceOf(AutoLock.LockedFSDataOutputStream.class);

      LOG.info("Output stream  {}:", stream2);
      stream2.write(0);

      LOG.info("building {}", builder1);
      // copy to reference in the lambda
      final FSDataOutputStream s2 = stream2;

      intercept(FileAlreadyExistsException.class, () -> {
        FSDataOutputStream stream1 = builder1.build();
        String err = "Expected builder1.build() of "
            + builder1 + " to fail but it returned a stream "
            + stream1 + " while builder2's stream is "
            + s2;
        LOG.error(err);
        return err;
      });
      stream2.close();
      // try twice
      stream2.close();

    } finally {
      if (stream2 != null) {
        stream2.close();
      }
      hboss.delete(path, false);
    }
  }

}
