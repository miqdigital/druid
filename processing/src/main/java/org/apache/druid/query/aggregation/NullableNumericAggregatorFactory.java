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

package org.apache.druid.query.aggregation;


import com.google.common.base.Preconditions;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.segment.BaseNullableColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;

/**
 * Abstract superclass for null-aware numeric aggregators.
 *
 * Includes functionality to wrap {@link Aggregator}, {@link BufferAggregator}, {@link VectorAggregator}, and
 * {@link AggregateCombiner} to support nullable aggregations. The result of this aggregator will be null if all the
 * values to be aggregated are null values, or if no values are aggregated at all. If any of the values are non-null,
 * the result will be the aggregated value of the non-null values.
 *
 * Aggregators that use {@link ColumnValueSelector#getObject()} must override
 * {@link #useGetObject(ColumnSelectorFactory)}. Otherwise, the logic in this class is not correct for
 * non-numeric selectors.
 *
 * @see BaseNullableColumnValueSelector#isNull() for why this only works in the numeric case
 */
@ExtensionPoint
public abstract class NullableNumericAggregatorFactory<T extends BaseNullableColumnValueSelector>
    extends AggregatorFactory
{
  /**
   * If true, this aggregator will not check for null inputs, instead directly delegating to the underlying aggregator.
   * Currently, this is only used by {@link CountAggregatorFactory#getCombiningFactory}.
   * This isn't entirely safe, as some selectors might throw an exception when attempting to retrieve a primitive value from `null`.
   */
  public boolean forceNotNullable()
  {
    return false;
  }

  @Override
  public final Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
  {
    T selector = selector(columnSelectorFactory);
    Aggregator aggregator = factorize(columnSelectorFactory, selector);
    if (this.forceNotNullable()) {
      return aggregator;
    }
    return new NullableNumericAggregator(aggregator, makeNullSelector(selector, columnSelectorFactory));
  }

  @Override
  public final BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    T selector = selector(columnSelectorFactory);
    BufferAggregator aggregator = factorizeBuffered(columnSelectorFactory, selector);
    if (this.forceNotNullable()) {
      return aggregator;
    }
    return new NullableNumericBufferAggregator(aggregator, makeNullSelector(selector, columnSelectorFactory));
  }

  @Override
  public final VectorAggregator factorizeVector(VectorColumnSelectorFactory columnSelectorFactory)
  {
    Preconditions.checkState(canVectorize(columnSelectorFactory), "Cannot vectorize");
    VectorValueSelector selector = vectorSelector(columnSelectorFactory);
    VectorAggregator aggregator = factorizeVector(columnSelectorFactory, selector);
    if (this.forceNotNullable()) {
      return aggregator;
    }
    return new NullableNumericVectorAggregator(aggregator, selector);
  }

  @Override
  public final AggregateCombiner makeNullableAggregateCombiner()
  {
    AggregateCombiner<?> combiner = makeAggregateCombiner();
    if (this.forceNotNullable()) {
      return combiner;
    }
    return new NullableNumericAggregateCombiner<>(combiner);
  }

  @Override
  public final int getMaxIntermediateSizeWithNulls()
  {
    if (this.forceNotNullable()) {
      return getMaxIntermediateSize();
    }
    return getMaxIntermediateSize() + Byte.BYTES;
  }

  /**
   * Returns the selector that should be used by {@link NullableNumericAggregator} and
   * {@link NullableNumericBufferAggregator} to determine if the current value is null.
   */
  private BaseNullableColumnValueSelector makeNullSelector(
      final T selector,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    if (useGetObject(columnSelectorFactory)) {
      final BaseObjectColumnValueSelector<?> objectSelector = (BaseObjectColumnValueSelector<?>) selector;
      return () -> objectSelector.getObject() == null;
    } else {
      return selector;
    }
  }

  // ---- ABSTRACT METHODS BELOW ------

  /**
   * Creates a {@link ColumnValueSelector} for the aggregated column.
   *
   * @see ColumnValueSelector
   */
  protected abstract T selector(ColumnSelectorFactory columnSelectorFactory);

  /**
   * Returns whether the selector created by {@link #selector(ColumnSelectorFactory)} for the given
   * {@link ColumnSelectorFactory} prefers {@link BaseObjectColumnValueSelector#getObject()}.
   *
   * For backwards compatibilty with older extensions, this is a non-abstract method.
   */
  protected boolean useGetObject(ColumnSelectorFactory columnSelectorFactory)
  {
    return false;
  }

  /**
   * Creates a {@link VectorValueSelector} for the aggregated column.
   *
   * @see VectorValueSelector
   */
  protected VectorValueSelector vectorSelector(VectorColumnSelectorFactory columnSelectorFactory)
  {
    throw new UnsupportedOperationException("Cannot vectorize");
  }

  /**
   * Creates an {@link Aggregator} to aggregate values from several rows, by using the provided selector.
   *
   * @param columnSelectorFactory metricFactory
   * @param selector              {@link ColumnValueSelector} for the column to aggregate.
   *
   * @see Aggregator
   */
  protected abstract Aggregator factorize(ColumnSelectorFactory columnSelectorFactory, T selector);

  /**
   * Creates an {@link BufferAggregator} to aggregate values from several rows into a ByteBuffer.
   *
   * @param columnSelectorFactory columnSelectorFactory in case any other columns are needed.
   * @param selector              {@link ColumnValueSelector} for the column to aggregate.
   *
   * @see BufferAggregator
   */
  protected abstract BufferAggregator factorizeBuffered(
      ColumnSelectorFactory columnSelectorFactory,
      T selector
  );

  /**
   * Creates a {@link VectorAggregator} to aggregate values from several rows into a ByteBuffer.
   *
   * @param columnSelectorFactory columnSelectorFactory in case any other columns are needed.
   * @param selector              {@link VectorValueSelector} for the column to aggregate.
   *
   * @see BufferAggregator
   */
  protected VectorAggregator factorizeVector(
      VectorColumnSelectorFactory columnSelectorFactory,
      VectorValueSelector selector
  )
  {
    if (!canVectorize(columnSelectorFactory)) {
      throw new UnsupportedOperationException("Cannot vectorize");
    } else {
      throw new UnsupportedOperationException("canVectorize returned true but 'factorizeVector' is not implemented");
    }
  }

  @Override
  public ColumnType getResultType()
  {
    return getIntermediateType();
  }
}
