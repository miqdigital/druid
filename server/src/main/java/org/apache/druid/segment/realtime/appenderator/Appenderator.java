/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.realtime.appenderator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.query.QuerySegmentWalker;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * An Appenderator indexes data. It has some in-memory data and some persisted-on-disk data. It can serve queries on
 * both of those. It can also push data to deep storage. But, it does not decide which segments data should go into.
 * It also doesn't publish segments to the metadata store or monitor handoff; you have to do that yourself!
 * <p>
 * You can provide a {@link Committer} or a Supplier of one when you call one of the methods that {@link #add},
 * {@link #persistAll}, or {@link #push}. The Committer should represent all data you have given to the Appenderator so
 * far. This Committer will be used when that data has been persisted to disk.
 *
 * Concurrency: all methods defined in this class directly, including {@link #close()} and {@link #closeNow()}, i. e.
 * all methods of the data appending and indexing lifecycle except {@link #drop} must be called from a single thread.
 * Methods inherited from {@link QuerySegmentWalker} can be called concurrently from multiple threads.
 *<p>
 * Important note: For historical reasons there was a single implementation for this interface ({@code AppenderatorImpl})
 * but that since has been split into two classes: {@link StreamAppenderator} and {@link BatchAppenderator}. With this change
 * all the query support & concurrency has been removed/changed in {@code BatchAppenderator} therefore this class no longer
 * makes sense to have as an {@code Appenderator}. In the future we may want to refactor away the {@code Appenderator}
 * interface from {@code BatchAppenderator}.
 */
public interface Appenderator extends QuerySegmentWalker
{
  /**
   * Return the identifier of this Appenderator; useful for log messages and such.
   */
  String getId();

  /**
   * Return the name of the dataSource associated with this Appenderator.
   */
  String getDataSource();

  /**
   * Perform any initial setup. Should be called before using any other methods.
   *
   * @return currently persisted commit metadata
   */
  Object startJob();

  /**
   * Same as {@link #add(SegmentIdWithShardSpec, InputRow, Supplier, boolean)}, with allowIncrementalPersists set to
   * true
   */
  default AppenderatorAddResult add(
      SegmentIdWithShardSpec identifier,
      InputRow row,
      Supplier<Committer> committerSupplier
  ) throws SegmentNotWritableException
  {
    return add(identifier, row, committerSupplier, true);
  }


  /**
   * Add a row. Must not be called concurrently from multiple threads.
   * <p>
   * If no pending segment exists for the provided identifier, a new one will be created.
   * <p>
   * This method may trigger a {@link #persistAll(Committer)} using the supplied Committer. If it does this, the
   * Committer is guaranteed to be *created* synchronously with the call to add, but will actually be used
   * asynchronously.
   * <p>
   * If committer is not provided, no metadata is persisted.
   *
   * @param identifier               the segment into which this row should be added
   * @param row                      the row to add
   * @param committerSupplier        supplier of a committer associated with all data that has been added, including
   *                                 this row if {@code allowIncrementalPersists} is set to false then this will not be
   *                                 used as no persist will be done automatically
   * @param allowIncrementalPersists indicate whether automatic persist should be performed or not if required.
   *                                 If this flag is set to false then the return value should have
   *                                 {@link AppenderatorAddResult#isPersistRequired} set to true if persist was skipped
   *                                 because of this flag and it is assumed that the responsibility of calling
   *                                 {@link #persistAll(Committer)} is on the caller.
   *
   * @return {@link AppenderatorAddResult}
   *
   * @throws SegmentNotWritableException if the requested segment is known, but has been closed
   */
  AppenderatorAddResult add(
      SegmentIdWithShardSpec identifier,
      InputRow row,
      @Nullable Supplier<Committer> committerSupplier,
      boolean allowIncrementalPersists
  )
      throws SegmentNotWritableException;

  /**
   * Returns a list of all currently active segments.
   */
  List<SegmentIdWithShardSpec> getSegments();

  /**
   * Returns the number of rows in a particular pending segment.
   *
   * @param identifier segment to examine
   *
   * @return row count
   *
   * @throws IllegalStateException if the segment is unknown
   */
  @VisibleForTesting
  int getRowCount(SegmentIdWithShardSpec identifier);

  /**
   * Returns the number of total rows in this appenderator of all segments pending push.
   *
   * @return total number of rows
   */
  int getTotalRowCount();

  /**
   * Drop all in-memory and on-disk data, and forget any previously-remembered commit metadata. This could be useful if,
   * for some reason, rows have been added that we do not actually want to hand off. Blocks until all data has been
   * cleared. This may take some time, since all pending persists must finish first.
   */
  @VisibleForTesting
  void clear() throws InterruptedException;

  /**
   * Schedule dropping all data associated with a particular pending segment. Unlike {@link #clear()}), any on-disk
   * commit metadata will remain unchanged. If there is no pending segment with this identifier, then this method will
   * do nothing.
   * <p>
   * You should not write to the dropped segment after calling "drop". If you need to drop all your data and
   * re-write it, consider {@link #clear()} instead.
   *
   * This method might be called concurrently from a thread different from the "main data appending / indexing thread",
   * from where all other methods in this class (except those inherited from {@link QuerySegmentWalker}) are called.
   * This typically happens when {@code drop()} is called in an async future callback. drop() itself is cheap
   * and relays heavy dropping work to an internal executor of this Appenderator.
   *
   * @param identifier the pending segment to drop
   *
   * @return future that resolves when data is dropped
   */
  ListenableFuture<?> drop(SegmentIdWithShardSpec identifier);

  /**
   * Persist any in-memory indexed data to durable storage. This may be only somewhat durable, e.g. the
   * machine's local disk. The Committer will be made synchronously with the call to persistAll, but will actually
   * be used asynchronously. Any metadata returned by the committer will be associated with the data persisted to
   * disk.
   * <p>
   * If committer is not provided, no metadata is persisted.
   *
   * @param committer a committer associated with all data that has been added so far
   *
   * @return future that resolves when all pending data has been persisted, contains commit metadata for this persist
   */
  ListenableFuture<Object> persistAll(@Nullable Committer committer);

  /**
   * Merge and push particular segments to deep storage. This will trigger an implicit
   * {@link #persistAll(Committer)} using the provided Committer.
   * <p>
   * After this method is called, you cannot add new data to any segments that were previously under construction.
   * <p>
   * If committer is not provided, no metadata is persisted.
   *
   * @param identifiers   list of segments to push
   * @param committer     a committer associated with all data that has been added so far
   * @param useUniquePath true if the segment should be written to a path with a unique identifier
   *
   * @return future that resolves when all segments have been pushed. The segment list will be the list of segments
   * that have been pushed and the commit metadata from the Committer.
   */
  ListenableFuture<SegmentsAndCommitMetadata> push(
      Collection<SegmentIdWithShardSpec> identifiers,
      @Nullable Committer committer,
      boolean useUniquePath
  );

  /**
   * Stop any currently-running processing and clean up after ourselves. This allows currently running persists and
   * pushes to finish. This will not remove any on-disk persisted data, but it will drop any data that has not yet been
   * persisted.
   */
  void close();

  /**
   * Stop all processing, abandoning current pushes, currently running persist may be allowed to finish if they persist
   * critical metadata otherwise shutdown immediately. This will not remove any on-disk persisted data,
   * but it will drop any data that has not yet been persisted.
   * Since this does not wait for pushes to finish, implementations have to make sure if any push is still happening
   * in background thread then it does not cause any problems.
   */
  void closeNow();

  /**
   * Sets thread context for task threads on Indexers. Since the {@link Appenderator}
   * and the underlying threadpools for persist, push, publish are freshly
   * created for each task ID, this context need not be cleared.
   */
  default void setTaskThreadContext()
  {

  }

  /**
   * Result of {@link Appenderator#add} containing following information
   * - {@link SegmentIdWithShardSpec} - identifier of segment to which rows are being added
   * - int - positive number indicating how many summarized rows exist in this segment so far and
   * - boolean - true if {@param allowIncrementalPersists} is set to false and persist is required; false otherwise
   */
  class AppenderatorAddResult
  {
    private final SegmentIdWithShardSpec segmentIdentifier;
    private final int numRowsInSegment;
    private final boolean isPersistRequired;

    AppenderatorAddResult(
        SegmentIdWithShardSpec identifier,
        int numRowsInSegment,
        boolean isPersistRequired
    )
    {
      this.segmentIdentifier = identifier;
      this.numRowsInSegment = numRowsInSegment;
      this.isPersistRequired = isPersistRequired;
    }

    SegmentIdWithShardSpec getSegmentIdentifier()
    {
      return segmentIdentifier;
    }

    @VisibleForTesting
    public int getNumRowsInSegment()
    {
      return numRowsInSegment;
    }

    boolean isPersistRequired()
    {
      return isPersistRequired;
    }
  }
}
