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

package org.apache.hadoop.hbase.oss.sync;

import java.io.IOException;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Provides convenience data structures to help ensure that locks are closed
 * when and only when the path is no longer in use. The basic AutoLock is simply
 * an AutoCloseable that will release the lock after a try-with-resources block.
 * LockedRemoteIterator will release the lock when the stream has been exhausted
 * or in the event of any exception. LockedFSDataOutputStream will release the
 * lock when the stream gets closed.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface AutoLock extends AutoCloseable {
  public void close() throws IOException;

  /**
   * A wrapper for a RemoteIterator that releases a lock only when the
   * underlying iterator has been exhausted.
   */
  public static class LockedRemoteIterator<E> implements RemoteIterator<E> {

    public LockedRemoteIterator(RemoteIterator<E> iterator, AutoLock lock)
          {
      this.iterator = iterator;
      this.lock = lock;
    }

    private RemoteIterator<E> iterator;
    private AutoLock lock;
    private AtomicBoolean closed = new AtomicBoolean(false);

    public void close() throws IOException {
      if (!closed.getAndSet(true)) {
        lock.close();
      }
    }

    private void checkClosed() throws IOException {
      if (closed.get()) {
        throw new IOException(
              "LockedRemoteIterator was accessed after releasing lock");
      }
    }

    @Override
    public boolean hasNext() throws IOException {
      checkClosed();
      try {
        if (iterator.hasNext()) {
          return true;
        }
        close();
        return false;
      } catch (Throwable e) {
        close();
        throw e;
      }
    }

    /**
     * Delegates to the wrapped iterator, but will close the lock in the event
     * of a NoSuchElementException. Some applications do not call hasNext() and
     * simply depend on the NoSuchElementException.
     */
    @Override
    public E next() throws IOException {
      checkClosed();
      try {
        return iterator.next();
      } catch (Throwable e) {
        close();
        throw e;
      }
    }
  }

  /**
   * A wrapper for a FSDataOutputStream that releases a lock only when the
   * underlying output stream is closed.
   */
  public class LockedFSDataOutputStream extends FSDataOutputStream {

    public LockedFSDataOutputStream(FSDataOutputStream stream, AutoLock lock) throws IOException {
      // super() throws IOException, but this constructor can't catch it.
      // Instantiators must catch the exception and close the lock.
      super(stream, null);
      this.stream = stream;
      this.lock = lock;
    }

    private final FSDataOutputStream stream;
    private final AutoLock lock;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private void checkClosed() throws IOException {
      if (closed.get()) {
        throw new IOException(
              "LockedFSDataOutputStream was accessed after releasing lock");
      }
    }

    @Override
    /**
     * Returns the position in the wrapped stream. This should not be accessed
     * after the stream has been closed. Unlike most other functions in this
     * class, this is not enforced because this function shouldn't throw
     * IOExceptions in Hadoop 3
     *
     * FSDataOutputStream.getPos() declares that it can throw IOExceptions in Hadoop
     * 2, but the implementation never does. So it could, in theory, but no
     * situation in which it actually would is know.
     */
    public long getPos() {
      try {
        return stream.getPos();
      } catch (Exception e) {
        // We can't specify IOException and still compile against Hadoop 3
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() throws IOException {
      // Contract tests attempt to close the stream twice
      if (!closed.getAndSet(true)) {
        try {
          stream.close();
        } finally {
          lock.close();
        }
      }
    }

    @Override
    public String toString() {
      return "LockedFSDataOutputStream:"
          + " closed=" + closed.get() + " "
          + stream.toString();
    }

    @Override
    /**
     * Returns the wrapped stream. This should not be accessed after the stream
     * has been closed. Unlike most other functions in this class, this is not
     * enforced because this function shouldn't throw IOExceptions.
     */
    public OutputStream getWrappedStream() {
      return stream.getWrappedStream();
    }

    @Override
    public void hflush() throws IOException {
      checkClosed();
      stream.hflush();
    }

    @Override
    public void hsync() throws IOException {
      checkClosed();
      stream.hsync();
    }

    @Override
    public void setDropBehind(Boolean dropBehind) throws IOException {
      checkClosed();
      stream.setDropBehind(dropBehind);
    }
  }
}
