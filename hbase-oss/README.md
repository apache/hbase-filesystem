<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# HBOSS: HBase / Object Store Semantics adapter

## Introduction

This module provides an implementation of Apache Hadoop's FileSystem interface
that bridges the gap between Apache HBase, which assumes that many operations
are atomic, and object-store implementations of FileSystem (such as s3a) which
inherently cannot provide atomic semantics to those operations natively.

This is implemented separately from s3a so that it can potentially be used for
other object stores. It is also impractical to provide the required semantics
for the general case without significant drawbacks in some cases. A separate
implementation allows all trade-offs to be made on HBase's terms.

## Lock Implementations

TreeLockManager implements generic logic for managing read / write locks on
branches of filesystem hierarchies, but needs to be extended by an
implementation that provides individual read / write locks and methods to
traverse the tree.

The desired implementation must be configured by setting to one of the values
below:

    fs.hboss.sync.impl

### Null Implementation (org.apache.hadoop.hbase.oss.sync.NullTreeLockManager)

The null implementation just provides no-op methods instead of actual locking
operations. This functions as an easy way to verify that a test case has
successfully reproduced a problem that is hidden by the other implementations.

### Local Implementation (org.apache.hadoop.hbase.oss.sync.LocalTreeLockManager)

Primarily intended to help with development and validation, but could possibly
work for a standalone instance of HBase. This implementation uses Java's
built-in ReentrantReadWriteLock.

### ZooKeeper Implementation (org.apache.hadoop.hbase.oss.sync.ZKTreeLockManager)

This implementation is intended for production use once it is stable. It uses
Apache Curator's implementation of read / write locks on Apache ZooKeeper. It
could share a ZooKeeper ensemble with the HBase cluster.

At a minimum, you must configure the ZooKeeper connection string (including
root znode):

    fs.hboss.sync.zk.connectionString

You may also want to configure:

    fs.hboss.sync.zk.sleep.base.ms (default 1000)
    fs.hboss.sync.zk.sleep.max.retries (default 3)

### DynamoDB Implementation (not implemented)

An implementation based on Amazon's DynamoDB lock library was considered but
was not completed due to the lack of an efficient way to traverse the tree and
discover locks on child nodes. The benefit is that S3Guard is required for s3a
use and as such there's a dependency on DynamoDB anyway.

## Storage Implementations

Currently HBOSS is primarily designed for and exclusively tested with Hadoop's
s3a client against Amazon S3. *S3Guard must be enabled, which is available in
Hadoop 2.9.0, 3.0.0, and higher*.

Both the use of S3Guard and Zookeeper for locking (i.e. Zookeeper) have
implications for other clients that are not configured to share the same
metadata store and Zookeeper ensemble. Ideally, all clients should be have the
same configuration in these respects. Read-only clients may not share these
resources with the HBase processes, but they will not have the added safety
provided by these features. Clients that do not share these resources and modify
data can compromise the correctness of HBase.


In theory, HBOSS could also work well with Google's cloud storage client (gs)
or other object storage clients, but this has not been tested.

## FileSystem Instantiation

There are 2 ways to get an HBOSS instance. It can be instantiated directly and
given a URI and Configuration object that can be used to get the underlying
FileSystem:

    Configuration conf = new Configuration();
    FileSystem hboss = new HBaseObjectStoreSemantics();
    hboss.initialize("s3a://bucket/", conf);

If the application code cannot be changed, you can remap the object store
client's schema to the HBOSS implementation, and set
fs.hboss.fs.&lt;scheme&gt;.impl to the underlying implementation that HBOSS
should wrap:

    Configuration conf = new Configuration();
    conf.set("fs.hboss.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.impl", "org.apache.hadoop.hbase.oss.HBaseObjectStoreSemantics");
    FileSystem hboss = FileSystem.get("s3a://bucket/", conf);

## Testing

You can quickly run HBOSS's tests with any of the implementations:

    mvn verify -Pnull  # reproduce the problems
    mvn verify -Plocal # useful for debugging
    mvn verify -Pzk    # the default

If the 'zk' profile is activated, it will start an embedded ZooKeeper process.
The tests can also be run against a distributed ZooKeeper ensemble by setting
fs.hboss.sync.zk.connectionString in src/test/resources/core-site.xml.

By default, the tests will also be run against a mock S3 client that works on
in-memory data structures. One can also set fs.hboss.data.uri to point to any
other storage in src/test/resources/core-site.xml.

Any required credentials or other individal configuration should be set in
src/test/resources/auth-keys.xml, which should be ignored by source control.

## Hadoop Versions

HBoss mainly depends on *org.apache.hadoop.fs.FileSystem* contract, and
current HBoss version is compatible with Hadoop releases *2.9.2* and *3.2.0*.

There are Maven profiles defined for Hadoop 2 and Hadoop 3 major versions.
These are activated via the property `hadoop.profile`. These profiles choose
a specific Hadoop release in that major line, defaulting to versions as defined
in `hadoop2.version` and `hadoop3.version`. By default, Hadoop 3 is used by
the build.

    mvn verify                    # Defaults to Hadoop 3
    mvn verify -Dhadoop.profile=3 # Activate Hadoop 3
    mvn verify -Dhadoop.profile=2 # Activate Hadoop 2
