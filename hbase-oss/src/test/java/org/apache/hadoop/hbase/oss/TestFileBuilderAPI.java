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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.oss.sync.AutoLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * test the createFile() and openFile builder APIs,
 * where the existence checks
 * take place in build(), not the FS API call.
 * This means the lock checking needs to be postponed.
 */
public class TestFileBuilderAPI extends HBaseObjectStoreSemanticsTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFileBuilderAPI.class);
  public static final int SLEEP_INTERVAL = 5_000;
  public static final int TIMEOUT = 15;
  public static final String EXPERIMENTAL_FADVISE = "fs.s3a.experimental.input.fadvise";


  /**
   * Prefix for all standard filesystem options: {@value}.
   */
  private static final String FILESYSTEM_OPTION = "fs.option.";

  /**
   * Prefix for all openFile options: {@value}.
   */
  public static final String FS_OPTION_OPENFILE =
      FILESYSTEM_OPTION + "openfile.";

  /**
   * OpenFile option for file length: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_LENGTH =
      FS_OPTION_OPENFILE + "length";

  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  /**
   * Create the file via the builder in separate threads, verifying that
   * the builder executed in the second thread is blocked
   * until the first thread finishes its write.
   */
  @Test
  public void testCreateOverlappingBuilders() throws Exception {
    Path path = testPath("testCreateOverlappingBuilders");

    FSDataOutputStream stream = null;
    try {
      FSDataOutputStreamBuilder builder = hboss.createFile(path)
          .overwrite(false);

      LOG.info("building {}:", builder);
      stream = builder.build();

      Assertions.assertThat(stream)
          .describedAs("expected a LockedFSDataOutputStream")
          .isInstanceOf(AutoLock.LockedFSDataOutputStream.class);

      LOG.info("Output stream  {}:", stream);
      stream.write(0);

      // submit the writer into a new thread

      Future<Long> fut = executor.submit(() -> {
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
          throw e;
        }
      });

      long streamCloseTimestamp = sleep();
      LOG.info("main thread closing stream");
      stream.close();
      // try twice for rigorousness
      stream.close();
      long threadTimestamp = fut.get(TIMEOUT, TimeUnit.SECONDS);
      Assertions.assertThat(threadTimestamp)
          .describedAs("timestamp of file creation in the second thread")
          .isGreaterThanOrEqualTo(streamCloseTimestamp);

    } finally {
      IOUtils.cleanupWithLogger(LOG, stream);
      hboss.delete(path, false);
    }
  }

  /**
   * Create the file via the builder in separate threads,
   * each without overwrite.
   * verify that the builder executed in the second thread
   * failed because the file exists.
   * Because in s3 the file doesn't exist until the write is finished,
   * this verifies that all existence probes are delayed until the
   * first write finishes
   */
  @Test
  public void testCreateNoOverwrite() throws Exception {
    Path path = testPath("testCreateNoOverwrite");

    FSDataOutputStream stream = null;
    try {
      FSDataOutputStreamBuilder builder = hboss.createFile(path)
          .overwrite(false);

      LOG.info("building {}:", builder);
      stream = builder.build();

      LOG.info("Output stream  {}:", stream);
      stream.write(0);

      // submit the writer into a new thread

      Future<Long> fut = executor.submit(() -> {
        FSDataOutputStreamBuilder builder1 = hboss.createFile(path)
            .overwrite(false);
        LOG.info("Inner thread building {}", builder1);
        try (FSDataOutputStream out = builder1.build()) {
          long executionTime = System.currentTimeMillis();
          LOG.info("Inner thread file created at {}", executionTime);
          out.write(Bytes.toBytes("localhost"));
          out.flush();
          return executionTime;
        }
      });

      sleep();
      stream.close();

      // the operation failed.
      ExecutionException ex = intercept(ExecutionException.class, () ->
          fut.get(TIMEOUT, TimeUnit.SECONDS));

      // and the inner exception was an overwrite failure
      // therefore the second file exists
      intercept(FileAlreadyExistsException.class, () -> {
        throw (Exception) ex.getCause();
      });

    } finally {
      IOUtils.cleanupWithLogger(LOG, stream);
      hboss.delete(path, false);
    }
  }

  /**
   * Create the file via the builder in one thread;
   * read in the second.
   * Compare the output.
   */
  @Test
  public void testCreateAndRead() throws Exception {
    Path path = testPath("testCreateAndRead");

    FSDataOutputStream stream = null;
    try {
      // do a full chain of options. so if there's a problem
      // with passthrough, we will see
      // the must calls get overridden by the opt ones called after,
      // so don't fail on creation
      //
      // FSDataOutputStreamBuilder has some problems with type
      // once some of the bulder methods are called.
      FSDataOutputStreamBuilder builder = (FSDataOutputStreamBuilder)
          hboss.createFile(path)
              .overwrite(false)
              .replication((short) 1)
              .bufferSize(20_000)
              .permission(FsPermission.getDefault())
              .recursive()
              .blockSize(32_000_000)
              .checksumOpt(Options.ChecksumOpt.createDisabled())
              .must("int", 0)
              .must("long", 0L)
              .must("string", "s")
              .must("double", 0.2f)
              .must("bool", false)
              .opt("int", 0)
              .opt("long", 0L)
              .opt("string", "s")
              .opt("double", 0.2f)
              .opt("bool", false);

      assertLockedCreateBuilder(builder);

      LOG.info("building {}:", builder);
      stream = builder.build();

      LOG.info("Output stream  {}:", stream);
      byte[] output = Bytes.toBytes("localhost");
      stream.write(output);

      // submit the writer into a new thread

      Future<Long> fut = executor.submit(() -> {
        FutureDataInputStreamBuilder builder1 = hboss.openFile(path)
            .must("int", 0)
            .must("long", 0L)
            .must("string", "s")
            .must("double", 0.2f)
            .must("bool", false)
            .opt("int", 0)
            .opt("long", 0L)
            .opt("string", "s")
            .opt("double", 0.2f)
            .opt("bool", false);
        LOG.info("Inner thread building {}", builder1);
        assertLockedOpenBuilder(builder1);
        try (FSDataInputStream in = builder1.build().get()) {
          long executionTime = System.currentTimeMillis();
          LOG.info("Inner thread file created at {}", executionTime);
          byte[] expected = new byte[output.length];
          in.readFully(0, expected);
          Assertions.assertThat(expected)
              .describedAs("data read from %s", path)
              .isEqualTo(output);
          LOG.info("Stream statistics {}",
              ioStatisticsToPrettyString(in.getIOStatistics()));
          return executionTime;
        }
      });
      long streamCloseTimestamp = sleep();
      LOG.info("main thread closing stream");
      stream.close();
      long threadTimestamp = fut.get(TIMEOUT, TimeUnit.SECONDS);
      Assertions.assertThat(threadTimestamp)
          .describedAs("timestamp of file creation in the second thread")
          .isGreaterThanOrEqualTo(streamCloseTimestamp);

    } finally {
      IOUtils.cleanupWithLogger(LOG, stream);
      hboss.delete(path, false);
    }
  }

  /**
   * Validate all the builder options and that they
   * get through to the interior.
   */
  @Test
  public void testCreateOptionPassthrough() {
    Path path = testPath("testCreateOptionPassthrough");

    LockedCreateFileBuilder builder = (LockedCreateFileBuilder)
        hboss.createFile(path)
            .overwrite(false)
            .create()
            .replication((short) 1)
            .bufferSize(20_000)
            .permission(FsPermission.getDefault())
            .recursive()
            .blockSize(32_000_000)
            .checksumOpt(Options.ChecksumOpt.createDisabled())
            .progress(() -> {
            })
            .must("m1", 0)
            .must("m2", 0L)
            .must("m3", "s")
            .must("m4", 0.2f)
            .must("m5", false)
            .opt("o1", 0)
            .opt("o2", 0L)
            .opt("o3", "s")
            .opt("o4", 0.2f)
            .opt("o5", false);

    // validate the view from the builder
    assertMandatoryKeysComplete(builder.getMandatoryKeys());
    assertAllKeysComplete(builder.getOptions());
    // and that the wrapped class is happy
    assertMandatoryKeysComplete(builder.getWrapped().getMandatoryKeys());
    assertAllKeysComplete(builder.getWrapped().getOptions());
    Assertions.assertThat(builder.getProgress())
        .isNotNull();
  }

  /**
   * Validate all the builder options and that they
   * get through to the interior.
   */
  @Test
  public void testOpenOptionPassthrough() throws Exception {
    Path path = testPath("testOpenOptionPassthrough");

    LockedOpenFileBuilder builder = (LockedOpenFileBuilder)
        hboss.openFile(path)
            .withFileStatus(null)
            .must("m1", 0)
            .must("m2", 0L)
            .must("m3", "s")
            .must("m4", 0.2f)
            .must("m5", false)
            .opt("o1", 0)
            .opt("o2", 0L)
            .opt("o3", "s")
            .opt("o4", 0.2f)
            .opt("o5", false);

    // validate the view from the builder
    assertMandatoryKeysComplete(builder.getMandatoryKeys());
    assertAllKeysComplete(builder.getOptions());
    Assertions.assertThat(builder.wasStatusPropagated())
        .describedAs("was status propagated")
        .isFalse();
  }

  /**
   * Use the filestatus with the read.
   * on s3 this should go all the way through
   */
  @Test
  public void testOpenOptionWithStatus() throws Exception {
    Path path = testPath("testOpenOptionWithStatus");
    try {
      ContractTestUtils.touch(hboss, path);
      FileStatus status = hboss.getFileStatus(path);

      LockedOpenFileBuilder builder =
          (LockedOpenFileBuilder) hboss.openFile(path)
              .withFileStatus(status)
              .opt(EXPERIMENTAL_FADVISE, "random");
      Assertions.assertThat(builder.wasStatusPropagated())
          .describedAs("was status %s propagated", status)
          .isTrue();
      readEmptyFile(builder);
      // if the fs was s3a, look more closely

      S3ABindingSupport s3ABindingSupport = new S3ABindingSupport(hboss.getRawFileSystem());
      if (s3ABindingSupport.isS3A()) {
        // try again with a different status
        LOG.info("Opening s3a file with different status than {}", status);
        LockedOpenFileBuilder builder2 =
            (LockedOpenFileBuilder) hboss.openFile(path)
                .withFileStatus(new FileStatus(status))
                .must(EXPERIMENTAL_FADVISE, "sequential")
                .opt(FS_OPTION_OPENFILE_LENGTH, status.getLen());  // HADOOP-16202
        Assertions.assertThat(builder2.wasStatusPropagated())
              .describedAs("was status %s propagated", status)
              .isFalse();
        readEmptyFile(builder2);
      }
    } finally {
      hboss.delete(path, false);
    }

  }

  private void readEmptyFile(LockedOpenFileBuilder builder)
      throws IOException, ExecutionException, InterruptedException {
    try (FSDataInputStream in = builder.build().get()) {
      Assertions.assertThat(in.read())
          .describedAs("read of empty file from %s", in)
          .isEqualTo(-1);
      LOG.info("Stream statistics {}",
          ioStatisticsToPrettyString(in.getIOStatistics()));
    }
  }
  /**
   * Assert that the optional and mandatory keys are present.
   *
   * @param options options to scan.
   */
  private void assertAllKeysComplete(final Configuration options) {
    List<String> list = new ArrayList<>();
    options.iterator().forEachRemaining(e ->
        list.add(e.getKey()));
    Assertions.assertThat(list)
        .describedAs("all keys in the builder")
        .containsExactlyInAnyOrder(
            "m1", "m2", "m3", "m4", "m5",
            "o1", "o2", "o3", "o4", "o5");
  }

  /**
   * Assert that the mandatory keys are present.
   *
   * @param mandatoryKeys mandatory keys
   */
  private void assertMandatoryKeysComplete(final Set<String> mandatoryKeys) {
    Assertions.assertThat(mandatoryKeys)
        .describedAs("mandatory keys in the builder")
        .containsExactlyInAnyOrder("m1", "m2", "m3", "m4", "m5");
  }

  private void assertLockedOpenBuilder(final FutureDataInputStreamBuilder builder1) {
    Assertions.assertThat(builder1)
        .isInstanceOf(LockedOpenFileBuilder.class);
  }

  private void assertLockedCreateBuilder(final FSDataOutputStreamBuilder builder) {
    Assertions.assertThat(builder)
        .isInstanceOf(LockedCreateFileBuilder.class);
  }

  /**
   * log and sleep.
   *
   * @return current time in millis.
   *
   * @throws InterruptedException if the sleep was interrupted.
   */
  private long sleep() throws InterruptedException {
    LOG.info("main thread sleeping");
    Thread.sleep(SLEEP_INTERVAL);
    LOG.info("main thread awake");
    return System.currentTimeMillis();
  }

}
