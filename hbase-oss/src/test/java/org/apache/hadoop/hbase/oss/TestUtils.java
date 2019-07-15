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

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.sync.EmbeddedZK;
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

  // This is defined by the Maven Surefire plugin configuration
  private static final String TEST_UNIQUE_FORK_ID = "test.unique.fork.id";

  private static EmbeddedZK zk = null;

  public static final String S3A = "s3a";

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
    // Newer versions of Hadoop will do this for us, but older ones won't
    // This allows Maven properties, profiles, etc. to set the implementation
    if (StringUtils.isEmpty(conf.get(Constants.SYNC_IMPL))) {
      conf.set(Constants.SYNC_IMPL, System.getProperty(Constants.SYNC_IMPL));
    }

    EmbeddedS3.conditionalStart(conf);
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
      e.printStackTrace();
      throw e;
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
}
