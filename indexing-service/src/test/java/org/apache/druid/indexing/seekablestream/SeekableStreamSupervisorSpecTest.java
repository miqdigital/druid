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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.supervisor.IdleConfig;
import org.apache.druid.indexing.seekablestream.supervisor.LagAggregator;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIngestionSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.LagBasedAutoScaler;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.LagBasedAutoScalerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.NoopTaskAutoScaler;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.TestSupervisorSpec;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertThrows;

public class SeekableStreamSupervisorSpecTest extends EasyMockSupport
{
  private SeekableStreamSupervisorIngestionSpec ingestionSchema;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private StubServiceEmitter emitter;
  private RowIngestionMetersFactory rowIngestionMetersFactory;
  private DataSchema dataSchema;
  private SeekableStreamSupervisorTuningConfig seekableStreamSupervisorTuningConfig;
  private SeekableStreamSupervisorIOConfig seekableStreamSupervisorIOConfig;
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private SeekableStreamIndexTaskClientFactory taskClientFactory;
  private static final String STREAM = "stream";
  private static final String DATASOURCE = "testDS";
  private static final String SUPERVISOR = "supervisor";
  private SeekableStreamSupervisorSpec spec;
  private SupervisorStateManagerConfig supervisorConfig;

  private SeekableStreamSupervisor supervisor4;

  private SeekableStreamIndexTaskClientFactory indexTaskClientFactory;
  private ObjectMapper mapper;
  private DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private SupervisorStateManagerConfig supervisorStateManagerConfig;

  @Before
  public void setUp()
  {
    ingestionSchema = EasyMock.mock(SeekableStreamSupervisorIngestionSpec.class);
    taskStorage = EasyMock.mock(TaskStorage.class);
    taskMaster = EasyMock.mock(TaskMaster.class);
    indexerMetadataStorageCoordinator = EasyMock.mock(IndexerMetadataStorageCoordinator.class);
    emitter = new StubServiceEmitter();
    rowIngestionMetersFactory = EasyMock.mock(RowIngestionMetersFactory.class);
    dataSchema = EasyMock.mock(DataSchema.class);
    seekableStreamSupervisorTuningConfig = EasyMock.mock(SeekableStreamSupervisorTuningConfig.class);
    seekableStreamSupervisorIOConfig = EasyMock.mock(SeekableStreamSupervisorIOConfig.class);
    taskClientFactory = EasyMock.mock(SeekableStreamIndexTaskClientFactory.class);
    spec = EasyMock.mock(SeekableStreamSupervisorSpec.class);
    supervisorConfig = new SupervisorStateManagerConfig();
    indexTaskClientFactory = EasyMock.mock(SeekableStreamIndexTaskClientFactory.class);
    mapper = new DefaultObjectMapper();
    monitorSchedulerConfig = EasyMock.mock(DruidMonitorSchedulerConfig.class);
    supervisorStateManagerConfig = EasyMock.mock(SupervisorStateManagerConfig.class);
    supervisor4 = EasyMock.mock(SeekableStreamSupervisor.class);

    EasyMock.expect(spec.getContextValue(DruidMetrics.TAGS)).andReturn(null).anyTimes();
  }

  private abstract class BaseTestSeekableStreamSupervisor extends SeekableStreamSupervisor<String, String, ByteEntity>
  {
    private BaseTestSeekableStreamSupervisor()
    {
      super(
          "testSupervisorId",
          taskStorage,
          taskMaster,
          indexerMetadataStorageCoordinator,
          taskClientFactory,
          OBJECT_MAPPER,
          spec,
          rowIngestionMetersFactory,
          false
      );
    }

    @Override
    protected String baseTaskName()
    {
      return "test";
    }

    @Override
    protected void updatePartitionLagFromStream()
    {
      // do nothing
    }

    @Nullable
    @Override
    protected Map<String, Long> getPartitionRecordLag()
    {
      return null;
    }

    @Nullable
    @Override
    protected Map<String, Long> getPartitionTimeLag()
    {
      return null;
    }

    @Override
    protected SeekableStreamIndexTaskIOConfig createTaskIoConfig(
        int groupId,
        Map<String, String> startPartitions,
        Map<String, String> endPartitions,
        String baseSequenceName,
        DateTime minimumMessageTime,
        DateTime maximumMessageTime,
        Set<String> exclusiveStartSequenceNumberPartitions,
        SeekableStreamSupervisorIOConfig ioConfig
    )
    {
      return new SeekableStreamIndexTaskIOConfig<>(
          groupId,
          baseSequenceName,
          new SeekableStreamStartSequenceNumbers<>(STREAM, startPartitions, exclusiveStartSequenceNumberPartitions),
          new SeekableStreamEndSequenceNumbers<>(STREAM, endPartitions),
          true,
          minimumMessageTime,
          maximumMessageTime,
          ioConfig.getInputFormat(),
          ioConfig.getTaskDuration().getStandardMinutes()
      )
      {
      };
    }

    @Override
    protected List<SeekableStreamIndexTask<String, String, ByteEntity>> createIndexTasks(
        int replicas,
        String baseSequenceName,
        ObjectMapper sortingMapper,
        TreeMap<Integer, Map<String, String>> sequenceOffsets,
        SeekableStreamIndexTaskIOConfig taskIoConfig,
        SeekableStreamIndexTaskTuningConfig taskTuningConfig,
        RowIngestionMetersFactory rowIngestionMetersFactory
    )
    {
      return null;
    }

    @Override
    protected int getTaskGroupIdForPartition(String partition)
    {
      return 0;
    }

    @Override
    protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata)
    {
      return true;
    }

    @Override
    protected boolean doesTaskMatchSupervisor(Task task)
    {
      return true;
    }

    @Override
    protected SeekableStreamDataSourceMetadata<String, String> createDataSourceMetaDataForReset(
        String stream,
        Map<String, String> map
    )
    {
      return null;
    }

    @Override
    protected OrderedSequenceNumber<String> makeSequenceNumber(String seq, boolean isExclusive)
    {
      return new OrderedSequenceNumber<>(seq, isExclusive)
      {
        @Override
        public int compareTo(OrderedSequenceNumber<String> o)
        {
          return new BigInteger(this.get()).compareTo(new BigInteger(o.get()));
        }
      };
    }

    @Override
    protected Map<String, Long> getRecordLagPerPartition(Map<String, String> currentOffsets)
    {
      return null;
    }

    @Override
    protected Map<String, Long> getTimeLagPerPartition(Map<String, String> currentOffsets)
    {
      return null;
    }

    @Override
    protected RecordSupplier<String, String, ByteEntity> setupRecordSupplier()
    {
      return recordSupplier;
    }

    @Override
    protected SeekableStreamSupervisorReportPayload<String, String> createReportPayload(
        int numPartitions,
        boolean includeOffsets
    )
    {
      return new SeekableStreamSupervisorReportPayload<>(
          SUPERVISOR,
          DATASOURCE,
          STREAM,
          1,
          1,
          1L,
          null,
          null,
          null,
          null,
          null,
          null,
          false,
          true,
          null,
          null,
          null
      )
      {
      };
    }

    @Override
    protected String getNotSetMarker()
    {
      return "NOT_SET";
    }

    @Override
    protected String getEndOfPartitionMarker()
    {
      return "EOF";
    }

    @Override
    protected boolean isEndOfShard(String seqNum)
    {
      return false;
    }

    @Override
    protected boolean isShardExpirationMarker(String seqNum)
    {
      return false;
    }

    @Override
    protected boolean useExclusiveStartSequenceNumberForNonFirstSequence()
    {
      return false;
    }
  }

  private class TestSeekableStreamSupervisor extends BaseTestSeekableStreamSupervisor
  {
    private int partitionNumbers;

    public TestSeekableStreamSupervisor(int partitionNumbers)
    {
      this.partitionNumbers = partitionNumbers;
    }

    @Override
    protected void scheduleReporting(ScheduledExecutorService reportingExec)
    {
      // do nothing
    }

    @Override
    public LagStats computeLagStats()
    {
      return new LagStats(0, 0, 0);
    }

    @Override
    public int getPartitionCount()
    {
      return partitionNumbers;
    }
  }

  private class StateOverrideTestSeekableStreamSupervisor extends TestSeekableStreamSupervisor
  {
    private SupervisorStateManager.State state;

    public StateOverrideTestSeekableStreamSupervisor(SupervisorStateManager.State state, int partitionNumbers)
    {
      super(partitionNumbers);
      this.state = state;
    }

    @Override
    public SupervisorStateManager.State getState()
    {
      return state;
    }
  }

  private static class TestSeekableStreamSupervisorSpec extends SeekableStreamSupervisorSpec
  {
    private SeekableStreamSupervisor supervisor;

    public TestSeekableStreamSupervisorSpec(
        SeekableStreamSupervisorIngestionSpec ingestionSchema,
        @Nullable Map<String, Object> context,
        Boolean suspended,
        TaskStorage taskStorage,
        TaskMaster taskMaster,
        IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
        SeekableStreamIndexTaskClientFactory indexTaskClientFactory,
        ObjectMapper mapper,
        ServiceEmitter emitter,
        DruidMonitorSchedulerConfig monitorSchedulerConfig,
        RowIngestionMetersFactory rowIngestionMetersFactory,
        SupervisorStateManagerConfig supervisorStateManagerConfig,
        SeekableStreamSupervisor supervisor,
        String id
    )
    {
      super(
          id,
          ingestionSchema,
          context,
          suspended,
          taskStorage,
          taskMaster,
          indexerMetadataStorageCoordinator,
          indexTaskClientFactory,
          mapper,
          emitter,
          monitorSchedulerConfig,
          rowIngestionMetersFactory,
          supervisorStateManagerConfig
      );

      this.supervisor = supervisor;
    }

    @Override
    public Supervisor createSupervisor()
    {
      return supervisor;
    }

    @Override
    public String getType()
    {
      return null;
    }

    @Override
    public String getSource()
    {
      return null;
    }

    @Override
    protected SeekableStreamSupervisorSpec toggleSuspend(boolean suspend)
    {
      return null;
    }
  }

  private static SeekableStreamSupervisorTuningConfig getTuningConfig()
  {
    return new SeekableStreamSupervisorTuningConfig()
    {
      @Override
      public Integer getWorkerThreads()
      {
        return 1;
      }

      @Override
      public Long getChatRetries()
      {
        return 1L;
      }

      @Override
      public Duration getHttpTimeout()
      {
        return new Period("PT1M").toStandardDuration();
      }

      @Override
      public Duration getShutdownTimeout()
      {
        return new Period("PT1S").toStandardDuration();
      }

      @Override
      public Duration getRepartitionTransitionDuration()
      {
        return new Period("PT2M").toStandardDuration();
      }

      @Override
      public Duration getOffsetFetchPeriod()
      {
        return new Period("PT5M").toStandardDuration();
      }

      @Override
      public SeekableStreamIndexTaskTuningConfig convertToTaskTuningConfig()
      {
        return new SeekableStreamIndexTaskTuningConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        )
        {
          @Override
          public SeekableStreamIndexTaskTuningConfig withBasePersistDirectory(File dir)
          {
            return null;
          }

          @Override
          public String toString()
          {
            return null;
          }
        };
      }
    };
  }

  @Test
  public void testAutoScalerConfig()
  {
    AutoScalerConfig autoScalerConfigEmpty = mapper.convertValue(new HashMap<>(), AutoScalerConfig.class);
    Assert.assertTrue(autoScalerConfigEmpty instanceof LagBasedAutoScalerConfig);
    Assert.assertFalse(autoScalerConfigEmpty.getEnableTaskAutoScaler());

    AutoScalerConfig autoScalerConfigNull = mapper.convertValue(null, AutoScalerConfig.class);
    Assert.assertNull(autoScalerConfigNull);

    AutoScalerConfig autoScalerConfigDefault = mapper.convertValue(
        ImmutableMap.of("autoScalerStrategy", "lagBased"),
        AutoScalerConfig.class
    );
    Assert.assertTrue(autoScalerConfigDefault instanceof LagBasedAutoScalerConfig);

    AutoScalerConfig autoScalerConfigValue = mapper.convertValue(
        ImmutableMap.of("lagCollectionIntervalMillis", "1"),
        AutoScalerConfig.class
    );
    Assert.assertTrue(autoScalerConfigValue instanceof LagBasedAutoScalerConfig);
    LagBasedAutoScalerConfig lagBasedAutoScalerConfig = (LagBasedAutoScalerConfig) autoScalerConfigValue;
    Assert.assertEquals(lagBasedAutoScalerConfig.getLagCollectionIntervalMillis(), 1);

    Exception e = null;
    try {
      AutoScalerConfig autoScalerError = mapper.convertValue(ImmutableMap.of(
          "enableTaskAutoScaler",
          "true",
          "taskCountMax",
          "1",
          "taskCountMin",
          "4"
      ), AutoScalerConfig.class);
    }
    catch (RuntimeException ex) {
      e = ex;
    }
    Assert.assertNotNull(e);

    e = null;
    try {
      // taskCountMax and taskCountMin couldn't be ignored.
      AutoScalerConfig autoScalerError2 = mapper.convertValue(
          ImmutableMap.of("enableTaskAutoScaler", "true"),
          AutoScalerConfig.class
      );
    }
    catch (RuntimeException ex) {
      e = ex;
    }
    Assert.assertNotNull(e);
  }

  @Test
  public void testAutoScalerCreated()
  {
    HashMap<String, Object> autoScalerConfig = new HashMap<>();
    autoScalerConfig.put("enableTaskAutoScaler", true);
    autoScalerConfig.put("lagCollectionIntervalMillis", 500);
    autoScalerConfig.put("lagCollectionRangeMillis", 500);
    autoScalerConfig.put("scaleOutThreshold", 5000000);
    autoScalerConfig.put("triggerScaleOutFractionThreshold", 0.3);
    autoScalerConfig.put("scaleInThreshold", 1000000);
    autoScalerConfig.put("triggerScaleInFractionThreshold", 0.8);
    autoScalerConfig.put("scaleActionStartDelayMillis", 0);
    autoScalerConfig.put("scaleActionPeriodMillis", 100);
    autoScalerConfig.put("taskCountMax", 8);
    autoScalerConfig.put("taskCountMin", 1);
    autoScalerConfig.put("scaleInStep", 1);
    autoScalerConfig.put("scaleOutStep", 2);
    autoScalerConfig.put("minTriggerScaleActionFrequencyMillis", 1200000);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(seekableStreamSupervisorIOConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(autoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.expect(seekableStreamSupervisorIOConfig.getStream()).andReturn("stream").anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);

    EasyMock.expect(supervisor4.getActiveTaskGroupsCount()).andReturn(0).anyTimes();
    EasyMock.expect(supervisor4.getIoConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.replay(supervisor4);

    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        null,
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    SupervisorTaskAutoScaler autoscaler = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler instanceof LagBasedAutoScaler);

    EasyMock.reset(seekableStreamSupervisorIOConfig);
    autoScalerConfig.put("enableTaskAutoScaler", false);
    EasyMock.expect(seekableStreamSupervisorIOConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(autoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);
    SupervisorTaskAutoScaler autoscaler2 = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler2 instanceof NoopTaskAutoScaler);

    EasyMock.reset(seekableStreamSupervisorIOConfig);
    autoScalerConfig.remove("enableTaskAutoScaler");
    EasyMock.expect(seekableStreamSupervisorIOConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(autoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);
    SupervisorTaskAutoScaler autoscaler3 = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler3 instanceof NoopTaskAutoScaler);

    EasyMock.reset(seekableStreamSupervisorIOConfig);
    autoScalerConfig.clear();
    EasyMock.expect(seekableStreamSupervisorIOConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(autoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);
    Assert.assertTrue(autoScalerConfig.isEmpty());
    SupervisorTaskAutoScaler autoscaler4 = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler4 instanceof NoopTaskAutoScaler);
  }

  @Test
  public void testDefaultAutoScalerConfigCreatedWithDefault()
  {
    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(seekableStreamSupervisorIOConfig.getAutoScalerConfig())
            .andReturn(mapper.convertValue(ImmutableMap.of(
                "lagCollectionIntervalMillis",
                "1",
                "enableTaskAutoScaler",
                true,
                "taskCountMax",
                "4",
                "taskCountMin",
                "1"
            ), AutoScalerConfig.class))
            .anyTimes();
    EasyMock.expect(seekableStreamSupervisorIOConfig.getStream()).andReturn("stream").anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);

    EasyMock.expect(supervisor4.getIoConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(supervisor4.getActiveTaskGroupsCount()).andReturn(0).anyTimes();
    EasyMock.replay(supervisor4);

    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        null,
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    SupervisorTaskAutoScaler autoscaler = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler instanceof LagBasedAutoScaler);
    LagBasedAutoScaler lagBasedAutoScaler = (LagBasedAutoScaler) autoscaler;
    LagBasedAutoScalerConfig lagBasedAutoScalerConfig = lagBasedAutoScaler.getAutoScalerConfig();
    Assert.assertEquals(lagBasedAutoScalerConfig.getLagCollectionIntervalMillis(), 1);
    Assert.assertEquals(lagBasedAutoScalerConfig.getLagCollectionRangeMillis(), 600000);
    Assert.assertEquals(lagBasedAutoScalerConfig.getScaleActionStartDelayMillis(), 300000);
    Assert.assertEquals(lagBasedAutoScalerConfig.getScaleActionPeriodMillis(), 60000);
    Assert.assertEquals(lagBasedAutoScalerConfig.getScaleOutThreshold(), 6000000);
    Assert.assertEquals(lagBasedAutoScalerConfig.getScaleInThreshold(), 1000000);
    Assert.assertEquals(lagBasedAutoScalerConfig.getTaskCountMax(), 4);
    Assert.assertEquals(lagBasedAutoScalerConfig.getTaskCountMin(), 1);
    Assert.assertEquals(lagBasedAutoScalerConfig.getScaleInStep(), 1);
    Assert.assertEquals(lagBasedAutoScalerConfig.getScaleOutStep(), 2);
    Assert.assertEquals(lagBasedAutoScalerConfig.getMinTriggerScaleActionFrequencyMillis(), 600000);
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleOut() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(1, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    StubServiceEmitter dynamicActionEmitter = new StubServiceEmitter();

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(10);

    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleOutProperties(10),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        dynamicActionEmitter
    );
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountBeforeScaleOut);
    Thread.sleep(1000);
    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(2, taskCountAfterScaleOut);
    Assert.assertTrue(
        dynamicActionEmitter
            .getMetricEvents(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC)
            .stream()
            .map(metric -> metric.getUserDims().get(SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION))
            .filter(Objects::nonNull)
            .anyMatch("minTriggerScaleActionFrequencyMillis not elapsed yet"::equals));
    emitter.verifyEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC, 1);
    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleOutAlreadyAtMax() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(2, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    StubServiceEmitter dynamicActionEmitter = new StubServiceEmitter();
    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(10)
    {
      @Override
      public int getActiveTaskGroupsCount()
      {
        return 2;
      }
    };

    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleOutProperties(2),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        dynamicActionEmitter
    );
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    Thread.sleep(1000);

    Assert.assertTrue(
        dynamicActionEmitter
            .getMetricEvents(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC)
            .stream()
            .map(metric -> metric.getUserDims().get(SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION))
            .filter(Objects::nonNull)
            .anyMatch("Already at max task count"::equals));
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithNoScalingOnIdleSupervisor() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(1, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new StateOverrideTestSeekableStreamSupervisor(
        SupervisorStateManager.BasicState.IDLE,
        3
    );

    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleOutProperties(2),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        emitter
    );
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountBeforeScaleOut);
    Thread.sleep(1000);
    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountAfterScaleOut);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleOutSmallPartitionNumber() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(1, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(2);
    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleOutProperties(3),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        emitter
    );
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountBeforeScaleOut);
    Thread.sleep(1000);
    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(2, taskCountAfterScaleOut);
    emitter.verifyEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC, 1);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleIn() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(2, false)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(3);
    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleInProperties(),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        emitter
    );

    // enable autoscaler so that taskcount config will be ignored and init value of taskCount will use taskCountMin.
    Assert.assertEquals(1, (int) supervisor.getIoConfig().getTaskCount());
    supervisor.getIoConfig().setTaskCount(2);
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(2, taskCountBeforeScaleOut);
    Thread.sleep(1000);
    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountAfterScaleOut);
    emitter.verifyEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC, 1);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleInThresholdGreaterThanPartitions() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(2, false)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(10);
    Map<String, Object> modifiedScaleInProps = getScaleInProperties();

    modifiedScaleInProps.put("taskCountMax", 20);
    modifiedScaleInProps.put("taskCountMin", 15);

    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            modifiedScaleInProps,
            LagBasedAutoScalerConfig.class
        ),
        spec,
        emitter
    );

    // enable autoscaler so that taskcount config will be ignored and init value of taskCount will use taskCountMin.
    Assert.assertEquals(1, (int) supervisor.getIoConfig().getTaskCount());
    supervisor.getIoConfig().setTaskCount(2);
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();

    Assert.assertEquals(2, (int) supervisor.getIoConfig().getTaskCount());
    Thread.sleep(2000);
    Assert.assertEquals(10, (int) supervisor.getIoConfig().getTaskCount());

    emitter.verifyEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC, 1);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleInAlreadyAtMin() throws InterruptedException
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(1, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    StubServiceEmitter dynamicActionEmitter = new StubServiceEmitter();
    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(10)
    {
      @Override
      public int getActiveTaskGroupsCount()
      {
        return 1;
      }
    };

    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(
        supervisor,
        DATASOURCE,
        mapper.convertValue(
            getScaleInProperties(),
            LagBasedAutoScalerConfig.class
        ),
        spec,
        dynamicActionEmitter
    );
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    Thread.sleep(1000);

    Assert.assertTrue(
        dynamicActionEmitter
            .getMetricEvents(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC)
            .stream()
            .map(metric -> metric.getUserDims().get(SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION))
            .filter(Objects::nonNull)
            .anyMatch("Already at min task count"::equals));
    emitter.verifyNotEmitted(SeekableStreamSupervisor.AUTOSCALER_SCALING_TIME_METRIC);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleDisable() throws InterruptedException
  {
    SeekableStreamSupervisorIOConfig seekableStreamSupervisorIOConfig = new SeekableStreamSupervisorIOConfig(
        "stream",
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        1,
        new Period("PT1H"),
        new Period("P1D"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        LagAggregator.DEFAULT,
        null,
        null,
        null
    )
    {
    };
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(this.seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(3);
    NoopTaskAutoScaler autoScaler = new NoopTaskAutoScaler();
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountBeforeScaleOut);
    Thread.sleep(1 * 1000);
    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountAfterScaleOut);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testEnablingIdleBeviourPerSupervisorWithOverlordConfigEnabled()
  {
    SeekableStreamSupervisorIOConfig seekableStreamSupervisorIOConfig = new SeekableStreamSupervisorIOConfig(
        "stream",
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        1,
        new Period("PT1H"),
        new Period("P1D"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        LagAggregator.DEFAULT,
        null,
        new IdleConfig(true, null),
        null
    )
    {
    };

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.replay(ingestionSchema);
    EasyMock.expect(dataSchema.getDataSource()).andReturn(DATASOURCE);
    EasyMock.replay(dataSchema);

    spec = new SeekableStreamSupervisorSpec(
        SUPERVISOR,
        ingestionSchema,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        supervisorStateManagerConfig
    )
    {
      @Override
      public Supervisor createSupervisor()
      {
        return null;
      }

      @Override
      protected SeekableStreamSupervisorSpec toggleSuspend(boolean suspend)
      {
        return null;
      }

      @Override
      public String getType()
      {
        return null;
      }

      @Override
      public String getSource()
      {
        return null;
      }
    };

    Assert.assertTrue(Objects.requireNonNull(spec.getIoConfig().getIdleConfig()).isEnabled());
  }

  @Test
  public void testGetContextVauleWithNullContextShouldReturnNull()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        null,
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    Assert.assertNull(spec.getContextValue("key"));
  }

  @Test
  public void testGetContextVauleForNonExistentKeyShouldReturnNull()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    Assert.assertNull(spec.getContextValue("key_not_exists"));
  }

  @Test
  public void testSupervisorIdEqualsDataSourceIfNull()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        SUPERVISOR
    );
    Assert.assertEquals(SUPERVISOR, spec.getId());
  }

  @Test
  public void testSupervisorIdDifferentFromDataSource()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        SUPERVISOR
    );
    Assert.assertEquals(SUPERVISOR, spec.getId());
  }

  @Test
  public void testGetContextVauleForKeyShouldReturnValue()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec spec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    Assert.assertEquals("value", spec.getContextValue("key"));
  }

  @Test
  public void test_validateSpecUpdateTo_ShortCircuits()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec originalSpec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        Map.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    TestSeekableStreamSupervisorSpec proposedSpec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        Map.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    );
    MatcherAssert.assertThat(
        assertThrows(DruidException.class, () -> originalSpec.validateSpecUpdateTo(proposedSpec)),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.INVALID_INPUT,
            "invalidInput"
        ).expectMessageIs(
            "Cannot update supervisor spec since one or both of the specs have not provided an input source stream in the 'ioConfig'."
        )
    );


    TestSupervisorSpec otherSpec = new TestSupervisorSpec("fake", new Object());
    MatcherAssert.assertThat(
        assertThrows(DruidException.class, () -> originalSpec.validateSpecUpdateTo(otherSpec)),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.INVALID_INPUT,
            "invalidInput"
        ).expectMessageIs(
            StringUtils.format("Cannot update supervisor spec from type[%s] to type[%s]", proposedSpec.getClass().getSimpleName(), otherSpec.getClass().getSimpleName())
        )
    );
  }

  @Test
  public void test_validateSpecUpdateTo_SourceStringComparisons()
  {
    mockIngestionSchema();
    TestSeekableStreamSupervisorSpec originalSpec = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    )
    {
      @Override
      public String getSource()
      {
        return "source1";
      }
    };
    TestSeekableStreamSupervisorSpec proposedSpecDiffSource = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    )
    {
      @Override
      public String getSource()
      {
        return "source2";
      }
    };
    TestSeekableStreamSupervisorSpec proposedSpecSameSource = new TestSeekableStreamSupervisorSpec(
        ingestionSchema,
        ImmutableMap.of("key", "value"),
        false,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig,
        supervisor4,
        "id1"
    )
    {
      @Override
      public String getSource()
      {
        return "source1";
      }
    };

    // Mistmatched stream strings test
    MatcherAssert.assertThat(
        assertThrows(DruidException.class, () -> originalSpec.validateSpecUpdateTo(proposedSpecDiffSource)),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.INVALID_INPUT,
            "invalidInput"
        ).expectMessageIs(
            "Update of the input source stream from [source1] to [source2] is not supported for a running supervisor."
            + "\nTo perform the update safely, follow these steps:"
            + "\n(1) Suspend this supervisor, reset its offsets and then terminate it. "
            + "\n(2) Create a new supervisor with the new input source stream."
            + "\nNote that doing the reset can cause data duplication or loss if any topic used in the old supervisor is included in the new one too."
        )
    );

    // Happy path test
    originalSpec.validateSpecUpdateTo(proposedSpecSameSource);
  }

  private void mockIngestionSchema()
  {
    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(dataSchema.getDataSource()).andReturn(DATASOURCE).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);
    EasyMock.replay(dataSchema);
  }

  private static DataSchema getDataSchema()
  {
    List<DimensionSchema> dimensions = new ArrayList<>();
    dimensions.add(StringDimensionSchema.create("dim1"));
    dimensions.add(StringDimensionSchema.create("dim2"));

    return DataSchema.builder()
                     .withDataSource(DATASOURCE)
                     .withTimestamp(new TimestampSpec("timestamp", "iso", null))
                     .withDimensions(dimensions)
                     .withAggregators(new CountAggregatorFactory("rows"))
                     .withGranularity(
                         new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.NONE,
                             ImmutableList.of()
                         )
                     )
                     .build();
  }

  private SeekableStreamSupervisorIOConfig getIOConfig(int taskCount, boolean scaleOut)
  {
    if (scaleOut) {
      return new SeekableStreamSupervisorIOConfig(
          "stream",
          new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
          1,
          taskCount,
          new Period("PT1H"),
          new Period("P1D"),
          new Period("PT30S"),
          false,
          new Period("PT30M"),
          null,
          null,
          mapper.convertValue(getScaleOutProperties(2), AutoScalerConfig.class),
          LagAggregator.DEFAULT,
          null,
          null,
          null
      )
      {
      };
    } else {
      return new SeekableStreamSupervisorIOConfig(
          "stream",
          new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
          1,
          taskCount,
          new Period("PT1H"),
          new Period("P1D"),
          new Period("PT30S"),
          false,
          new Period("PT30M"),
          null,
          null,
          mapper.convertValue(getScaleInProperties(), AutoScalerConfig.class),
          LagAggregator.DEFAULT,
          null,
          null,
          null
      )
      {
      };
    }
  }

  private static Map<String, Object> getScaleOutProperties(int maxTaskCount)
  {
    HashMap<String, Object> autoScalerConfig = new HashMap<>();
    autoScalerConfig.put("enableTaskAutoScaler", true);
    autoScalerConfig.put("lagCollectionIntervalMillis", 50);
    autoScalerConfig.put("lagCollectionRangeMillis", 500);
    autoScalerConfig.put("scaleOutThreshold", 0);
    autoScalerConfig.put("triggerScaleOutFractionThreshold", 0.0);
    autoScalerConfig.put("scaleInThreshold", 1000000);
    autoScalerConfig.put("triggerScaleInFractionThreshold", 0.8);
    autoScalerConfig.put("scaleActionStartDelayMillis", 0);
    autoScalerConfig.put("scaleActionPeriodMillis", 100);
    autoScalerConfig.put("taskCountMax", maxTaskCount);
    autoScalerConfig.put("taskCountMin", 1);
    autoScalerConfig.put("scaleInStep", 1);
    autoScalerConfig.put("scaleOutStep", 2);
    autoScalerConfig.put("minTriggerScaleActionFrequencyMillis", 1200000);
    return autoScalerConfig;
  }

  private static Map<String, Object> getScaleInProperties()
  {
    HashMap<String, Object> autoScalerConfig = new HashMap<>();
    autoScalerConfig.put("enableTaskAutoScaler", true);
    autoScalerConfig.put("lagCollectionIntervalMillis", 500);
    autoScalerConfig.put("lagCollectionRangeMillis", 500);
    autoScalerConfig.put("scaleOutThreshold", 8000000);
    autoScalerConfig.put("triggerScaleOutFractionThreshold", 0.3);
    autoScalerConfig.put("scaleInThreshold", 0);
    autoScalerConfig.put("triggerScaleInFractionThreshold", 0.0);
    autoScalerConfig.put("scaleActionStartDelayMillis", 0);
    autoScalerConfig.put("scaleActionPeriodMillis", 100);
    autoScalerConfig.put("taskCountMax", 2);
    autoScalerConfig.put("taskCountMin", 1);
    autoScalerConfig.put("scaleInStep", 1);
    autoScalerConfig.put("scaleOutStep", 2);
    autoScalerConfig.put("minTriggerScaleActionFrequencyMillis", 1200000);
    return autoScalerConfig;
  }

}
