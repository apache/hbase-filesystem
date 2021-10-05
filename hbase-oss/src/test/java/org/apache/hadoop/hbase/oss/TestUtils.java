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

import static org.apache.hadoop.fs.s3a.Constants.S3_CLIENT_FACTORY_IMPL;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.hbase.oss.Constants.DATA_URI;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore;
import org.apache.hadoop.hbase.oss.sync.EmbeddedZK;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.junit.Assume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TestUtils {
  public static final Logger LOG =
        LoggerFactory.getLogger(TestUtils.class);

  public static enum HadoopVersion {
    HADOOP32("3.2"),
    HADOOP33("3.3");

    private final String versionIdentifier;

    HadoopVersion(String versionIdentifier) {
      this.versionIdentifier = versionIdentifier;
    }

    public String getIdentifier() {
      return versionIdentifier;
    }
  }

  // This is defined by the Maven Surefire plugin configuration
  private static final String TEST_UNIQUE_FORK_ID = "test.unique.fork.id";

  private static EmbeddedZK zk = null;

  public static final String S3A = "s3a";

  public static boolean usingEmbeddedS3 = false;

  public static void conditionalStart(Configuration conf) {
    if (StringUtils.isEmpty(conf.get(S3_METADATA_STORE_IMPL))) {
      conf.set(S3_METADATA_STORE_IMPL, LocalMetadataStore.class.getName());
    }

    boolean notConfigured = StringUtils.isEmpty(conf.get(DATA_URI));
    if (notConfigured) {
      usingEmbeddedS3 = true;
      conf.set(S3_CLIENT_FACTORY_IMPL, getEmbeddedS3ClientFactoryClassName());
      conf.set(DATA_URI, "s3a://" + EmbeddedS3.BUCKET);
    } else {
      usingEmbeddedS3 = false;
    }
  }

  public static void addContract(Configuration conf) {
    final HadoopVersion version = getDesiredHadoopVersion();
    String contractFile;
    switch (version) {
    case HADOOP32:
      contractFile = "contract/hadoop-3.2/s3a.xml";
      break;
    case HADOOP33:
      contractFile = "contract/hadoop-3.3/s3a.xml";
      break;
    default:
      throw new RuntimeException("Unhandled HadoopVersion: " + version);
    }
    URL url = TestUtils.class.getClassLoader().getResource(contractFile);
    if (url == null) {
      throw new RuntimeException("Failed to find s3a contract file on classpath: " + contractFile);
    }
    LOG.info("Adding s3a contract definition: {}", contractFile);
    conf.addResource(contractFile);
  }

  /**
   * Returns the class name for the S3ClientFactory implementation for the
   * given major version of Hadoop.
   */
  public static String getEmbeddedS3ClientFactoryClassName() {
    final HadoopVersion version = getDesiredHadoopVersion();
    switch (version) {
    case HADOOP32:
      return "org.apache.hadoop.hbase.oss.Hadoop32EmbeddedS3ClientFactory";
    case HADOOP33:
      return "org.apache.hadoop.hbase.oss.Hadoop33EmbeddedS3ClientFactory";
    }

    throw new RuntimeException("HadoopVersion " + version + " is not handled.");
  }

  /**
   * Attempts to return a HadoopVersion enum value given the value of the system
   * property {@code HBOSS_HADOOP_VERSION}. This system property is set via
   * the pom.xml via the corresponding profile for each Hadoop version this project
   * has support for.
   */
  static HadoopVersion getDesiredHadoopVersion() {
    String hadoopVersPropValue = System.getProperty("HBOSS_HADOOP_VERSION");
    if (hadoopVersPropValue == null) {
      throw new RuntimeException("HBOSS_HADOOP_VERSION was not set as a system property.");
    }
    for (HadoopVersion version : HadoopVersion.values()) {
      if (hadoopVersPropValue.equals(version.getIdentifier())) {
        return version;
      }
    }

    LOG.error("Found HBOSS_HADOOP_VERSION property set to '{}',"
        + "but there is no corresponding HadoopVersion enum value", hadoopVersPropValue);
    throw new RuntimeException("Unable to determine S3ClientFactory to instantiate");
  }

  public static boolean renameToExistingDestinationSupported() {
    HadoopVersion version = getDesiredHadoopVersion();
    // Hadoop 3.2 and below don't support the additional checks added
    // by HADOOP-16721 around renames.
    if (version == HadoopVersion.HADOOP32) {
      return false;
    }
    return true;
  }

  public static String getScheme(Configuration conf) {
    String dataUri = conf.get(Constants.DATA_URI);
    try {
      return new URI(dataUri).getScheme();
    } catch (URISyntaxException e) {
      return null;
    }
  }

  public static boolean fsIs(String scheme, Configuration conf) {
    return getScheme(conf).equals(scheme);
  }

  public static void disableFilesystemCaching(Configuration conf) {
    String property = "fs." + getScheme(conf) + ".impl.disable.cache";
    conf.setBoolean(property, true);
  }

  public static Path testPathRoot(HBaseObjectStoreSemantics hboss) {
      String testUniqueForkId = System.getProperty(TEST_UNIQUE_FORK_ID);
    String root = "/hboss-junit";
    if (testUniqueForkId != null) {
      root += "-" + testUniqueForkId;
    }
    return new Path(hboss.getHomeDirectory(), root);
  }

  public static Path testPath(HBaseObjectStoreSemantics hboss, String path) {
    return testPath(hboss, new Path(path));
  }

  public static Path testPath(HBaseObjectStoreSemantics hboss, Path path) {
    return new Path(testPathRoot(hboss), path);
  }

  public static HBaseObjectStoreSemantics getFileSystem(Configuration conf) throws Exception {
    // Prevent re-registration of the same MetricsSource
    DefaultMetricsSystem.setMiniClusterMode(true);

    patchFileSystemImplementation(conf);

    conditionalStart(conf);
    synchronized (TestUtils.class) {
      if (zk == null) {
        zk = new EmbeddedZK();
      }
    }
    zk.conditionalStart(conf);

    try {
      String dataURI = conf.get(Constants.DATA_URI);
      Assume.assumeNotNull(dataURI);
      URI name = new URI(dataURI);
      HBaseObjectStoreSemantics hboss = new HBaseObjectStoreSemantics();
      hboss.initialize(name, conf);
      return hboss;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw e;
    }
  }

  /**
   * Pick up the fs.hboss.sync.impl value from the JVM system property,
   * which is how it is passed down from maven.
   * If this isn't set, fall back to the local tree lock.
   * That enables IDE test runs.
   * @param conf configuration to patch.
   */
  private static void patchFileSystemImplementation(Configuration conf) {
    // Newer versions of Hadoop will do this for us, but older ones won't
    // This allows Maven properties, profiles, etc. to set the implementation
    if (StringUtils.isEmpty(conf.get(Constants.SYNC_IMPL))) {
      String property = System.getProperty(Constants.SYNC_IMPL);
      if (property == null) {
        property = "org.apache.hadoop.hbase.oss.sync.LocalTreeLockManager";
      }
      conf.set(Constants.SYNC_IMPL, property);
    }
  }

  public static void cleanup(HBaseObjectStoreSemantics hboss) throws Exception {
    if (hboss != null) {
      hboss.close();
    }
    synchronized (TestUtils.class) {
      if (zk != null) {
        zk.conditionalStop();
      }
    }
  }

  public static void runIfS3(boolean isS3, Configuration conf) {
    try {
      TestUtils.getFileSystem(conf);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
    if(isS3) {
      Assume.assumeTrue(TestUtils.fsIs(TestUtils.S3A, conf));
    } else {
      Assume.assumeFalse(TestUtils.fsIs(TestUtils.S3A, conf));
    }
  }
}
