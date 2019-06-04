
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
# HBASE Changelog

## Release hbase-filesystem-1.0.0-alpha1 - Unreleased (as of 2019-06-04)



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HBASE-22149](https://issues.apache.org/jira/browse/HBASE-22149) | HBOSS: A FileSystem implementation to provide HBase's required semantics on object stores |  Critical | Filesystem Integration | Sean Mackrory | Sean Mackrory |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HBASE-22437](https://issues.apache.org/jira/browse/HBASE-22437) | HBOSS: Add Hadoop 2 / 3 profiles |  Major | hboss | Sean Mackrory | Sean Mackrory |
| [HBASE-22415](https://issues.apache.org/jira/browse/HBASE-22415) | HBOSS: Reduce log verbosity in ZKTreeLockManager when waiting on a parent/child node lock |  Minor | . | Wellington Chevreuil | Wellington Chevreuil |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HBASE-22386](https://issues.apache.org/jira/browse/HBASE-22386) | HBOSS: Limit depth that listing locks check for other locks |  Major | . | Sean Mackrory | Sean Mackrory |
| [HBASE-22416](https://issues.apache.org/jira/browse/HBASE-22416) | HBOSS: unit tests fail with ConnectionLoss when IPv6 enabled and not set up locally |  Minor | Filesystem Integration | Josh Elser | Josh Elser |
| [HBASE-22393](https://issues.apache.org/jira/browse/HBASE-22393) | HBOSS: Shaded external dependencies to avoid conflicts with Hadoop and HBase |  Critical | Filesystem Integration | Sean Mackrory | Sean Mackrory |
| [HBASE-22427](https://issues.apache.org/jira/browse/HBASE-22427) | HBOSS: TestTreeLockManager fails on non-ZK implementations |  Major | Filesystem Integration | Sean Mackrory | Sean Mackrory |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HBASE-22493](https://issues.apache.org/jira/browse/HBASE-22493) | HBOSS: Document supported hadoop versions. |  Minor | documentation, hboss | Wellington Chevreuil | Wellington Chevreuil |
| [HBASE-22515](https://issues.apache.org/jira/browse/HBASE-22515) | Document HBOSS test cases known to fail under Null lock implementation |  Minor | Filesystem Integration | Sean Busbey | Wellington Chevreuil |


