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

package org.apache.hadoop.hbase.oss;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Support for functional programming/lambda-expressions.
 * Lifted from org.apache.hadoop.fs.impl.FunctionsRaisingIOE;
 * it's in 3.3.0+, but now deprecated; having a copy isolates
 * from any changes
 */
public final class FunctionsRaisingIOE {

  private FunctionsRaisingIOE() {
  }

  /**
   * Function of arity 1 which may raise an IOException.
   * @param <T> type of arg1
   * @param <R> type of return value.
   */
  @FunctionalInterface
  public interface FunctionRaisingIOE<T, R> {

    R apply(T t) throws IOException;
  }

  /**
   * Function of arity 2 which may raise an IOException.
   * @param <T> type of arg1
   * @param <U> type of arg2
   * @param <R> type of return value.
   */
  @FunctionalInterface
  public interface BiFunctionRaisingIOE<T, U, R> {

    R apply(T t, U u) throws IOException;
  }

  /**
   * This is a callable which only raises an IOException.
   * @param <R> return type
   */
  @FunctionalInterface
  public interface CallableRaisingIOE<R> {

    R apply() throws IOException;
  }

}
