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

package org.apache.druid.indexing.overlord.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Global configurations for task lock. Used by the overlord.
 * This config takes precedence if it has a conflicting config with {@link DefaultTaskConfig}.
 */
public class TaskLockConfig
{
  @JsonProperty
  private boolean forceTimeChunkLock = true;

  @JsonProperty
  private boolean batchSegmentAllocation = true;

  @JsonProperty
  private long batchAllocationWaitTime = 0L;

  @JsonProperty
  private boolean batchAllocationReduceMetadataIO = true;

  @JsonProperty
  private int batchAllocationNumThreads = 5;

  public boolean isForceTimeChunkLock()
  {
    return forceTimeChunkLock;
  }

  public boolean isBatchSegmentAllocation()
  {
    return batchSegmentAllocation;
  }

  public long getBatchAllocationWaitTime()
  {
    return batchAllocationWaitTime;
  }

  public boolean isBatchAllocationReduceMetadataIO()
  {
    return batchAllocationReduceMetadataIO;
  }

  public int getBatchAllocationNumThreads()
  {
    return Math.max(1, batchAllocationNumThreads);
  }
}
