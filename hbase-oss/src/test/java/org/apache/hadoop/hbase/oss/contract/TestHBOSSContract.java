/*
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

package org.apache.hadoop.hbase.oss.contract;

import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.HBaseObjectStoreSemantics;
import org.apache.hadoop.hbase.oss.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assume.assumeTrue;

public class TestHBOSSContract extends FileSystemContractBaseTest {

  private Path basePath;
  private Configuration conf;

  @Rule
  public TestName methodName = new TestName();

  private void nameThread() {
    Thread.currentThread().setName("JUnit-" + methodName.getMethodName());
  }

  @Before
  public void setUp() throws Exception {
    nameThread();
    conf = new Configuration();
    conf.addResource("contract/s3a.xml");
    fs = TestUtils.getFileSystem(conf);
    Assume.assumeNotNull(fs);
    HBaseObjectStoreSemantics hboss = (HBaseObjectStoreSemantics)fs;
    basePath = fs.makeQualified(TestUtils.testPath(hboss, "ITestHBOSSContract"));
  }

  @Override
  public Path getTestBaseDir() {
    return basePath;
  }

  @Test
  public void testMkdirsWithUmask() throws Exception {
    // Skipped in the hadoop-aws tests
    if (TestUtils.fsIs(TestUtils.S3A, conf)) {
      // It would be nice to use Assume.assumeFalse instead of if, but Hadoop 2
      // builds pull in JUnit 3, and this is the only way to skip the test.
      return;
    }
    super.testMkdirsWithUmask();
  }

  @Test
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    if (!TestUtils.fsIs(TestUtils.S3A, conf)) {
      super.testRenameDirectoryAsExistingDirectory();
      return;
    }

    // Overridden implementation in the hadoop-aws tests
    Assume.assumeTrue(renameSupported());

    Path src = path("testRenameDirectoryAsExisting/dir");
    fs.mkdirs(src);
    createFile(path(src + "/file1"));
    createFile(path(src + "/subdir/file2"));

    Path dst = path("testRenameDirectoryAsExistingNew/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    Assert.assertFalse("Nested file1 exists",
        fs.exists(path(src + "/file1")));
    Assert.assertFalse("Nested file2 exists",
        fs.exists(path(src + "/subdir/file2")));
    Assert.assertTrue("Renamed nested file1 exists",
        fs.exists(path(dst + "/file1")));
    Assert.assertTrue("Renamed nested exists",
        fs.exists(path(dst + "/subdir/file2")));
  }

  @Test
  public void testMoveDirUnderParent() throws Throwable {
    // Skipped in the hadoop-aws tests
    if (TestUtils.fsIs(TestUtils.S3A, conf)) {
      // It would be nice to use Assume.assumeFalse instead of if, but Hadoop 2
      // builds pull in JUnit 3, and this is the only way to skip the test.
      return;
    }

    // Can't just call super.testMoveDirUnderParent() because it doesn't
    // exist in older Hadoop versions
    String methodName = "testMoveDirUnderParent";
    Method method = null;
    boolean skip = false;
    try {
      method = super.getClass().getMethod(methodName, (Class<?>[]) null);
    } catch (NoSuchMethodException e) {
      skip = true;
    }
    Assume.assumeFalse("Unable to find method " + methodName, skip);
    if (!skip) {
      method.invoke(this, (Object[]) null);
    }
  }

  @Test
  public void testRenameDirectoryMoveToNonExistentDirectory()
      throws Exception {
    skip("does not fail on S3A since HADOOP-16721");
  }

  @Test
  public void testRenameFileMoveToNonExistentDirectory() throws Exception {
    skip("does not fail on S3A since HADOOP-16721");
  }

  @Test
  public void testRenameDirectoryAsExistingFile() throws Exception {
    assumeTrue(renameSupported());

    Path src = path("testRenameDirectoryAsExistingFile/dir");
    fs.mkdirs(src);
    Path dst = path("testRenameDirectoryAsExistingFileNew/newfile");
    createFile(dst);
    intercept(FileAlreadyExistsException.class,
        () -> rename(src, dst, false, true, true));
  }

  @Test
  public void testRenameFileAsExistingFile() throws Exception {
    intercept(FileAlreadyExistsException.class,
        () -> super.testRenameFileAsExistingFile());
  }

  @Test
  public void testRenameNonExistentPath() throws Exception {
    intercept(FileNotFoundException.class,
        () -> super.testRenameNonExistentPath());

  }
}
