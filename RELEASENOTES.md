
<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->
# HBASE  hbase-filesystem-1.0.0-alpha1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HBASE-22149](https://issues.apache.org/jira/browse/HBASE-22149) | *Critical* | **HBOSS: A FileSystem implementation to provide HBase's required semantics on object stores**

<!-- markdown -->

Initial implementation of the hbase-oss module. Defines a wrapper implementation of Apache Hadoop's FileSystem interface that bridges the gap between Apache HBase, which assumes that many operations are atomic, and object-store implementations of FileSystem (such as s3a) which inherently cannot provide atomic semantics to those operations natively.

The implementation can be used e.g. with the s3a filesystem by using a root fs like `s3a://bucket/` and defining

* `fs.s3a.impl`  set to `org.apache.hadoop.hbase.oss.HBaseObjectStoreSemantics`
* `fs.hboss.fs.s3a.impl` set to `org.apache.hadoop.fs.s3a.S3AFileSystem`

more details in the module's README.md

NOTE: This module is labeled with an ALPHA version. It is not considered production ready and makes no promises about compatibility between versions.


---

* [HBASE-22393](https://issues.apache.org/jira/browse/HBASE-22393) | *Critical* | **HBOSS: Shaded external dependencies to avoid conflicts with Hadoop and HBase**

<!-- markdown -->

HBOSS now generates a single jar file that can be dropped into an HBase installation. This jar contains the thirdparty dependencies needed for HBOSS's implementation and communication with ZK (for the ZK Lock Manager).

HBOSS still relies on both HBase and Hadoop jars to be present at runtime. It also relies on the slf4j-api jar. It no longer includes an slf4j binding by default.



