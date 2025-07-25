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

package org.apache.druid.query.timeseries;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.CursorGranularizer;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.vector.VectorCursorGranularizer;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Cursors;
import org.apache.druid.segment.SegmentMissingException;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 *
 */
public class TimeseriesQueryEngine
{
  private final NonBlockingPool<ByteBuffer> bufferPool;

  /**
   * Constructor for tests. In production, the @Inject constructor is used instead.
   */
  @VisibleForTesting
  public TimeseriesQueryEngine()
  {
    this.bufferPool = new StupidPool<>("dummy", () -> ByteBuffer.allocate(10000000));
  }

  @Inject
  public TimeseriesQueryEngine(
      final @Global NonBlockingPool<ByteBuffer> bufferPool
  )
  {
    this.bufferPool = bufferPool;
  }

  /**
   * Run a single-segment, single-interval timeseries query on a particular adapter. The query must have been
   * scoped down to a single interval before calling this method.
   */
  public Sequence<Result<TimeseriesResultValue>> process(
      TimeseriesQuery query,
      final CursorFactory cursorFactory,
      @Nullable TimeBoundaryInspector timeBoundaryInspector,
      @Nullable final TimeseriesQueryMetrics timeseriesQueryMetrics
  )
  {
    if (cursorFactory == null) {
      throw new SegmentMissingException(
          "Null cursor factory found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final Interval interval = Iterables.getOnlyElement(query.getIntervals());
    final Granularity gran = query.getGranularity();

    final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(makeCursorBuildSpec(query, timeseriesQueryMetrics));
    if (cursorHolder.isPreAggregated()) {
      query = query.withAggregatorSpecs(Preconditions.checkNotNull(cursorHolder.getAggregatorsForPreAggregated()));
    }
    try {
      final Sequence<Result<TimeseriesResultValue>> result;

      if (query.context().getVectorize().shouldVectorize(cursorHolder.canVectorize())) {
        result = processVectorized(query, cursorHolder, timeBoundaryInspector, interval, gran);
      } else {
        result = processNonVectorized(query, cursorHolder, timeBoundaryInspector, interval, gran);
      }

      final int limit = query.getLimit();
      if (limit < Integer.MAX_VALUE) {
        return result.limit(limit).withBaggage(cursorHolder);
      } else {
        return result.withBaggage(cursorHolder);
      }
    }
    catch (Throwable t) {
      cursorHolder.close();
      throw t;
    }
  }

  private Sequence<Result<TimeseriesResultValue>> processVectorized(
      final TimeseriesQuery query,
      final CursorHolder cursorHolder,
      @Nullable final TimeBoundaryInspector timeBoundaryInspector,
      final Interval queryInterval,
      final Granularity gran
  )
  {
    final boolean skipEmptyBuckets = query.isSkipEmptyBuckets();
    final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();

    final VectorCursor cursor = cursorHolder.asVectorCursor();

    if (cursor == null) {
      return Sequences.empty();
    }

    final Closer closer = Closer.create();
    try {
      final VectorCursorGranularizer granularizer = VectorCursorGranularizer.create(
          cursor,
          timeBoundaryInspector,
          cursorHolder.getTimeOrder(),
          gran,
          queryInterval
      );

      if (granularizer == null) {
        return Sequences.withBaggage(Sequences.empty(), closer);
      }

      final VectorColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
      final AggregatorAdapters aggregators =
          AggregatorAdapters.factorizeVector(columnSelectorFactory, query.getAggregatorSpecs());
      closer.register(aggregators::reset);

      final ResourceHolder<ByteBuffer> bufferHolder = closer.register(bufferPool.take());

      final ByteBuffer buffer = bufferHolder.get();

      if (aggregators.spaceNeeded() > buffer.remaining()) {
        throw new ISE(
            "Not enough space for aggregators, needed [%,d] bytes but have only [%,d].",
            aggregators.spaceNeeded(),
            buffer.remaining()
        );
      }

      return Sequences.withBaggage(
          Sequences
              .simple(granularizer.getBucketIterable())
              .map(
                  bucketInterval -> {
                    // Whether or not the current bucket is empty
                    boolean emptyBucket = true;

                    while (!cursor.isDone()) {
                      granularizer.setCurrentOffsets(bucketInterval);

                      if (granularizer.getEndOffset() > granularizer.getStartOffset()) {
                        if (emptyBucket) {
                          aggregators.init(buffer, 0);
                        }

                        aggregators.aggregateVector(
                            buffer,
                            0,
                            granularizer.getStartOffset(),
                            granularizer.getEndOffset()
                        );

                        emptyBucket = false;
                      }

                      if (!granularizer.advanceCursorWithinBucket()) {
                        break;
                      }
                    }

                    if (emptyBucket && skipEmptyBuckets) {
                      // Return null, will get filtered out later by the Objects::nonNull filter.
                      return null;
                    }

                    final TimeseriesResultBuilder bob = new TimeseriesResultBuilder(
                        gran.toDateTime(bucketInterval.getStartMillis())
                    );

                    if (emptyBucket) {
                      aggregators.init(buffer, 0);
                    }

                    for (int i = 0; i < aggregatorSpecs.size(); i++) {
                      bob.addMetric(
                          aggregatorSpecs.get(i).getName(),
                          aggregators.get(buffer, 0, i)
                      );
                    }

                    return bob.build();
                  }
              )
              .filter(Objects::nonNull),
          closer
      );
    }
    catch (Throwable t1) {
      try {
        closer.close();
      }
      catch (Throwable t2) {
        t1.addSuppressed(t2);
      }
      throw t1;
    }
  }

  private Sequence<Result<TimeseriesResultValue>> processNonVectorized(
      final TimeseriesQuery query,
      final CursorHolder cursorHolder,
      @Nullable TimeBoundaryInspector timeBoundaryInspector,
      final Interval queryInterval,
      final Granularity gran
  )
  {
    final boolean skipEmptyBuckets = query.isSkipEmptyBuckets();
    final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
    final Cursor cursor = cursorHolder.asCursor();
    if (cursor == null) {
      return Sequences.empty();
    }
    final CursorGranularizer granularizer = CursorGranularizer.create(
        cursor,
        timeBoundaryInspector,
        cursorHolder.getTimeOrder(),
        gran,
        queryInterval
    );
    if (granularizer == null) {
      return Sequences.empty();
    }
    final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
    return Sequences.simple(granularizer.getBucketIterable())
                    .map(
                        bucketInterval -> {
                          // Whether or not the current bucket is empty
                          boolean emptyBucket = true;
                          boolean advancedToBucket = granularizer.advanceToBucket(bucketInterval);
                          if ((!advancedToBucket || cursor.isDone()) && skipEmptyBuckets) {
                            return null;
                          }
                          final Aggregator[] aggregators = new Aggregator[aggregatorSpecs.size()];
                          final String[] aggregatorNames = new String[aggregatorSpecs.size()];

                          for (int i = 0; i < aggregatorSpecs.size(); i++) {
                            aggregators[i] = aggregatorSpecs.get(i).factorize(columnSelectorFactory);
                            aggregatorNames[i] = aggregatorSpecs.get(i).getName();
                          }
                          try {
                            if (advancedToBucket) {
                              while (!cursor.isDone()) {
                                for (Aggregator aggregator : aggregators) {
                                  aggregator.aggregate();
                                }
                                emptyBucket = false;

                                if (!granularizer.advanceCursorWithinBucket()) {
                                  break;
                                }
                              }
                            }

                            if (emptyBucket && skipEmptyBuckets) {
                              // Return null, will get filtered out later by the Objects::nonNull filter.
                              return null;
                            }

                            final TimeseriesResultBuilder bob = new TimeseriesResultBuilder(
                                gran.toDateTime(bucketInterval.getStartMillis())
                            );
                            for (int i = 0; i < aggregatorSpecs.size(); i++) {
                              bob.addMetric(aggregatorNames[i], aggregators[i].get());
                            }

                            return bob.build();
                          }
                          finally {
                            // cleanup
                            for (Aggregator agg : aggregators) {
                              agg.close();
                            }
                          }
                        }
                    )
                    .filter(Objects::nonNull);
  }

  public static CursorBuildSpec makeCursorBuildSpec(TimeseriesQuery query, @Nullable QueryMetrics<?> queryMetrics)
  {
    return Granularities.decorateCursorBuildSpec(
        query,
        CursorBuildSpec.builder()
                       .setInterval(query.getSingleInterval())
                       .setFilter(Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter())))
                       .setVirtualColumns(query.getVirtualColumns())
                       .setPhysicalColumns(query.getRequiredColumns())
                       .setAggregators(query.getAggregatorSpecs())
                       .setQueryContext(query.context())
                       .setPreferredOrdering(
                           query.getGranularity().equals(Granularities.ALL)
                           ? List.of()
                           : query.isDescending() ? Cursors.descendingTimeOrder() : Cursors.ascendingTimeOrder()
                       )
                       .setQueryMetrics(queryMetrics)
                       .build()
    );
  }
}
