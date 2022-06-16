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
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.hbase.oss.sync.AutoLock;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager;

import static java.util.Objects.requireNonNull;

/**
 * a builder which wraps another FSBuilder and locks the
 * final build operation.
 * It also supports a transform of the wrapped result
 * for advanced processing.
 *
 * @param <S> type of built item
 * @param <B> builder interface
 */
public class LockingFSBuilderWrapper<S, B extends FSBuilder<S, B>>
    extends AbstractFSBuilderImpl<S, B> {

  private static final Logger LOG =
        LoggerFactory.getLogger(LockingFSBuilderWrapper.class);
  /**
   * Target path.
   */
  private final Path path;

  /**
   * Lock.
   */
  private final TreeLockManager sync;

  /**
   * Wrapped builder.
   */
  private final B wrapped;

  /**
   * A function which is invoked on the output of the wrapped build,
   * inside the lock operation.
   */
  private final FunctionsRaisingIOE.FunctionRaisingIOE<S, S> afterBuildTransform;

  /**
   * Constructor.
   * @param path Target path.
   * @param sync Lock.
   * @param wrapped Wrapped builder.
   * @param afterBuildTransform a function which is invoked on the output of the
   * wrapped build, inside the lock operation.
   *
   */
  public LockingFSBuilderWrapper(@Nonnull final Path path,
      final TreeLockManager sync,
      final B wrapped,
      final FunctionsRaisingIOE.FunctionRaisingIOE<S, S> afterBuildTransform) {

    super(path);
    this.sync = requireNonNull(sync);
    this.path = requireNonNull(path);
    this.wrapped = requireNonNull(wrapped);
    this.afterBuildTransform = requireNonNull(afterBuildTransform);
  }

  /**
   * Build the result.
   * @return result of wrapped build.
   * @throws IOException failure.
   */
  @Override
  public S build()
      throws IOException {

    LOG.debug("building stream for  {}:", path);
    try (AutoLock l = sync.lock(path)) {
      S result = afterBuildTransform.apply(wrapped.build());
      LOG.debug("result is {}:", result);
      return result;
    }
  }

  /**
   * Get the wrapped builder.
   * @return wrapped builder.
   */
  protected B getWrapped() {
    return wrapped;
  }

  @Override
  public B opt(@Nonnull final String key,
      @Nonnull final String value) {
    LOG.debug("{}: option {}=\"{}\"", path, key, value);
    wrapped.opt(key, value);
    return getThisBuilder();
  }

  @Override
  public B opt(@Nonnull final String key, final boolean value) {
    LOG.debug("{}: option {}=\"{}\"", path, key, value);
    wrapped.opt(key, value);
    return getThisBuilder();
  }

  @Override
  public B opt(@Nonnull final String key, final int value) {
    LOG.debug("{}: option {}=\"{}\"", path, key, value);
    wrapped.opt(key, value);
    return getThisBuilder();
  }

  @Override
  public B opt(@Nonnull final String key, final float value) {
    LOG.debug("{}: option {}=\"{}\"", path, key, value);
    wrapped.opt(key, value);
    return getThisBuilder();
  }

  @Override
  public B opt(@Nonnull final String key, final double value) {
    LOG.debug("{}: option {}=\"{}\"", path, key, value);
    wrapped.opt(key, value);
    return getThisBuilder();
  }

  @Override
  public B opt(@Nonnull final String key,
      @Nonnull final String... values) {
    LOG.debug("{}: option {}=(values)", path, key);
    wrapped.opt(key, values);
    return getThisBuilder();
  }

  @Override
  public B must(@Nonnull final String key,
      @Nonnull final String value) {
    wrapped.must(key, value);
    return getThisBuilder();
  }

  @Override
  public B must(@Nonnull final String key, final boolean value) {
    LOG.debug("{}: must {}=\"{}\"", path, key, value);
    wrapped.must(key, value);
    return getThisBuilder();
  }

  @Override
  public B must(@Nonnull final String key, final int value) {
    LOG.debug("{}: must {}=\"{}\"", path, key, value);
    wrapped.must(key, value);
    return getThisBuilder();
  }

  @Override
  public B must(@Nonnull final String key, final float value) {
    LOG.debug("{}: must {}=\"{}\"", path, key, value);
    wrapped.must(key, value);
    return getThisBuilder();
  }

  @Override
  public B must(@Nonnull final String key, final double value) {
    LOG.debug("{}: must {}=\"{}\"", path, key, value);
    wrapped.must(key, value);
    return getThisBuilder();
  }

  @Override
  public B must(@Nonnull final String key,
      @Nonnull final String... values) {
    LOG.debug("{}: must {}=(values)", path, key);
    wrapped.must(key, values);
    return getThisBuilder();
  }

  /**
   * Configure with a long value.
   * opt(String, Long) was not on the original interface,
   * though it is in recent hadoop builds.
   * It is implemented in the wrapper by converting
   * to a string and calling the wrapper's
   * {@code #opt(String, String)}.
   * It is NOT declared @Override so still compiles and
   * runs against hadoop 3.3.0.
   * @param key key to set
   * @param value long value
   * @return the builder
   */
  public B opt(@Nonnull String key, long value) {
    return opt(key, Long.toString(value));
  }

  /**
   * Configure with a long value.
   * must(String, Long) was not on the original interface,
   * though it is in recent hadoop builds.
   * It is implemented in the wrapper by converting
   * to a string and calling the wrapper's
   * {@code #must(String, String)}.
   * It is NOT declared @Override so still compiles and
   * runs against hadoop 3.3.0.
   * @param key key to set
   * @param value long value
   * @return the builder
   */
  public B must(@Nonnull String key, long value) {
    return must(key, Long.toString(value));
  }

  @Override
  public String toString() {
    return "LockingFSBuilderWrapper{" +
        "path=" + path +
        ", wrapped=" + wrapped +
        "} " + super.toString();
  }
}
