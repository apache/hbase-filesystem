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
import java.net.URI;

import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.hbase.oss.EmbeddedS3.EmbeddedAmazonS3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;

/**
 * An S3ClientFactory for Hadoop 3.3 releases which have the change from
 * HADOOP-13551. Builds on top of Hadoop32EmbeddedS3ClientFactory.
 */
public class Hadoop33EmbeddedS3ClientFactory implements S3ClientFactory {

  public AmazonS3 createS3Client(URI name) {
    AmazonS3 s3 = new EmbeddedAmazonS3();
    s3.createBucket(EmbeddedS3.BUCKET);
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

  public AmazonS3 createS3Client(URI uri,
      S3ClientCreationParameters s3ClientCreationParameters)
      throws IOException {
    AmazonS3 s3 = new EmbeddedAmazonS3();
    s3.createBucket(uri.getHost());
    return s3;
  }
}