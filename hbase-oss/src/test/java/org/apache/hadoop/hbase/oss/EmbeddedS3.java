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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.oss.Constants.*;
import static org.apache.hadoop.fs.s3a.Constants.*;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class EmbeddedS3 {

  public static boolean usingEmbeddedS3 = false;

  private static final String BUCKET = "embedded";

  public static void conditionalStart(Configuration conf) {
    if (StringUtils.isEmpty(conf.get(S3_METADATA_STORE_IMPL))) {
      conf.set(S3_METADATA_STORE_IMPL, LocalMetadataStore.class.getName());
    }

    boolean notConfigured = StringUtils.isEmpty(conf.get(DATA_URI));
    if (notConfigured) {
      usingEmbeddedS3 = true;
      conf.set(S3_CLIENT_FACTORY_IMPL,
            EmbeddedS3ClientFactory.class.getName());
      conf.set(DATA_URI, "s3a://" + BUCKET);
    } else {
      usingEmbeddedS3 = false;
    }
  }

  /**
   * Replaces the default S3ClientFactory to inject an EmbeddedAmazonS3
   * instance. This is currently a private API in Hadoop, but is the same method
   * used by S3Guard's inconsistency-injection tests. The method signature
   * defined in the interface varies depending on the Hadoop version.
   *
   * Due to compatibility purposes for both hadoop 2 and 3 main versions,
   * we are omitting "@override" annotation from overridden methods.
   */
  public static class EmbeddedS3ClientFactory implements S3ClientFactory {
    public AmazonS3 createS3Client(URI name) {
      AmazonS3 s3 = new EmbeddedAmazonS3();
      s3.createBucket(BUCKET);
      return s3;
    }

    public AmazonS3 createS3Client(URI uri,
        S3ClientCreationParameters s3ClientCreationParameters)
        throws IOException {
      AmazonS3 s3 = new EmbeddedAmazonS3();
      s3.createBucket(uri.getHost());
      return s3;
    }

    public AmazonS3 createS3Client(URI name,
        String bucket,
        AWSCredentialsProvider credentialSet,
        String userAgentSuffix) {
      AmazonS3 s3 = new EmbeddedAmazonS3();
      s3.createBucket(bucket);
      return s3;
    }

    public AmazonS3 createS3Client(URI name,
        String bucket,
        AWSCredentialsProvider credentialSet) {
      return createS3Client(name);
    }
  }

  /**
   * Emulates an S3-connected client. This is the bare minimum implementation
   * required for s3a to pass the contract tests while continuing to reproduce
   * such quirks as delayed file creation and non-atomic / slow renames.
   * Specifically, the following features are not supported:
   * <ul>
   *   <li>Multiple buckets</li>
   *   <li>Requester-pays</li>
   *   <li>Encryption</li>
   *   <li>Object versioning</li>
   *   <li>Multi-part</li>
   *   <li>ACLs</li>
   *   <li>Objects larger than Integer.MAX_VALUE</li>
   * </ul>
   */
  public static class EmbeddedAmazonS3 extends AbstractAmazonS3 {

    public static final Logger LOG =
          LoggerFactory.getLogger(EmbeddedAmazonS3.class);

    private String bucketName = null;

    // Randomized contract test datasets are generated byte-by-byte, so we must
    // ensure it's encoded as 8-bit characters
    private static final String ISO_8859_1 = "ISO-8859-1";
    private Charset encoding = Charset.forName(ISO_8859_1);

    private class EmbeddedS3Object extends S3Object {
      private ObjectMetadata meta;
      private String data;
      private long[] range;

      public EmbeddedS3Object() {
        super();
      }

      public EmbeddedS3Object(EmbeddedS3Object that, long[] range) {
        super();
        this.meta = that.meta;
        this.data = that.data;
        this.range = range;
      }

      public S3ObjectInputStream getObjectContent() {
        String substring = data.substring((int)range[0], (int)range[1]+1);
        InputStream in = IOUtils.toInputStream(substring, encoding);
        return new S3ObjectInputStream(in, null);
      }
    }

    private Map<String, EmbeddedS3Object> bucket = new ConcurrentHashMap<>();

    private void simulateServerSideCopy() {
      try {
        // For realism, this could be a function of data size, but 1/100s is
        // more than enough to reliably observe non-atomic renames.
        Thread.sleep(10);
      } catch (InterruptedException e) {}
    }

    // AmazonS3 interface below

    public CopyObjectResult copyObject(CopyObjectRequest request) {
      String sourceKey = request.getSourceKey();
      String destinationKey = request.getDestinationKey();
      LOG.debug("copyObject: {} -> {}", sourceKey, destinationKey);
      EmbeddedS3Object object = bucket.get(sourceKey);
      simulateServerSideCopy();
      bucket.put(destinationKey, object);
      return new CopyObjectResult();
    }

    public Bucket createBucket(String bucketName) {
      LOG.debug("createBucket: {}", bucketName);
      this.bucketName = bucketName;
      Bucket bucket = new Bucket(bucketName);
      return bucket;
    }

    public void deleteObject(String bucketName, String key) {
      LOG.debug("deleteObject: {}", key);
      bucket.remove(key);
    }

    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) {
      for (DeleteObjectsRequest.KeyVersion keyVersion : request.getKeys()) {
        String key = keyVersion.getKey();
        LOG.debug("deleteObjects: {}", key);
        bucket.remove(key);
      }
      return new DeleteObjectsResult(
            new ArrayList<DeleteObjectsResult.DeletedObject>());
    }

    public boolean doesBucketExist(String bucketName) {
      LOG.debug("doesBucketExist: {}", bucketName);
      if (this.bucketName == null) {
        this.bucketName = bucketName;
      }
      return this.bucketName.equals(bucketName);
    }

    public boolean doesBucketExistV2(String bucketName) {
      return this.doesBucketExist(bucketName);
    }

    public boolean doesObjectExist(String bucketName, String objectName)  {
      LOG.debug("doesObjectExist: {}", objectName);
      return bucket.containsKey(objectName);
    }

    public String getBucketLocation(String bucketName) {
      LOG.debug("getBucketLocation: {}", bucketName);
      // This is Region.US_Standard, but it's .toString() returns null
      return "us-east-1";
    }

    public S3Object getObject(GetObjectRequest request) {
      String key = request.getKey();
      long[] range = request.getRange();
      if (range.length != 2) {
        throw new IllegalArgumentException("Range must have 2 elements!");
      }
      LOG.debug("getObject: {} [{} - {}]", key, range[0], range[1]);
      EmbeddedS3Object object = bucket.get(key);
      return new EmbeddedS3Object(object, range);
    }

    public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest request) {
      String key = request.getKey();
      LOG.debug("getObjectMetadata: {}", key);
      if (!bucket.containsKey(key)) {
        AmazonServiceException e = new AmazonServiceException("404");
        e.setStatusCode(404);
        throw e;
      }
      EmbeddedS3Object object = bucket.get(key);
      return object.getObjectMetadata();
    }

    public ObjectListing listObjects(ListObjectsRequest request) {
      String prefix = request.getPrefix();
      String delimiter = request.getDelimiter();
      LOG.debug("listObjects: {} (delimiter = {})", prefix, delimiter);

      ObjectListing result = new ObjectListing();
      innerListing(prefix, delimiter, result.getObjectSummaries(),
            result.getCommonPrefixes());
      return result;
    }

    public ListObjectsV2Result listObjectsV2(ListObjectsV2Request request) {
      String prefix = request.getPrefix();
      String delimiter = request.getDelimiter();
      LOG.debug("listObjectsV2: {} (delimiter = {})", prefix, delimiter);

      ListObjectsV2Result result = new ListObjectsV2Result();
      innerListing(prefix, delimiter, result.getObjectSummaries(),
            result.getCommonPrefixes());
      return result;
    }

    private void innerListing(String prefix, String delimiter,
          List<S3ObjectSummary> summaries, List<String> prefixes) {
      Set<String> commonPrefixes = new HashSet<>();
      for (String key : bucket.keySet()) {
        if (!key.startsWith(prefix)) {
          continue;
        }
        if (delimiter != null) {
          int index = key.indexOf(delimiter, prefix.length());
          if (index > 0) {
            // Finding the delimiter means non-recursive listing
            // Add the first-level child to common prefixes and continue
            commonPrefixes.add(key.substring(0, index));
            continue;
          }
        }
        S3ObjectSummary summary = new S3ObjectSummary();
        summary.setKey(key);
        summary.setSize(bucket.get(key).data.length());
        summaries.add(summary);
      }
      prefixes.addAll(commonPrefixes);
    }

    public PutObjectResult putObject(PutObjectRequest request) {
      String key = request.getKey();
      LOG.debug("putObject: {}", key);

      EmbeddedS3Object object = bucket.get(key);
      if (object == null) {
        object = new EmbeddedS3Object();
      }

      try {
        if (request.getInputStream() != null) {
          InputStream in = request.getInputStream();
          object.data = IOUtils.toString(in, encoding);
          in.close();
        } else if (request.getFile() != null) {
          File file = request.getFile();
          object.data = FileUtils.readFileToString(file, encoding);
        } else {
          throw new UnsupportedOperationException("Unknown putObject method");
        }
      } catch (IOException e) {
        throw new SdkClientException("Error reading object to put", e);
      }
      // TODO later Hadoop versions will require setting ETags, etc. too
      object.getObjectMetadata().setContentLength(object.data.length());

      // Object isn't listed in the bucket until after it's written
      bucket.put(key, object);
      return new PutObjectResult();
    }

    public void shutdown() {
      LOG.debug("shutdown");
    }
  }
}
