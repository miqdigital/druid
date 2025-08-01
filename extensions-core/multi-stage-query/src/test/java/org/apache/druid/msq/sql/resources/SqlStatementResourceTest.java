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

package org.apache.druid.msq.sql.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.indexing.report.MSQTaskReportTest;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.msq.sql.SqlStatementState;
import org.apache.druid.msq.sql.entity.ColumnNameAndTypes;
import org.apache.druid.msq.sql.entity.PageInformation;
import org.apache.druid.msq.sql.entity.ResultSetInformation;
import org.apache.druid.msq.sql.entity.SqlStatementResult;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlResourceTest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;

public class SqlStatementResourceTest extends MSQTestBase
{
  public static final DateTime CREATED_TIME = DateTimes.of("2023-05-31T12:00Z");
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  private static final String ACCEPTED_SELECT_MSQ_QUERY = "QUERY_ID_1";
  private static final String RUNNING_SELECT_MSQ_QUERY = "QUERY_ID_2";
  private static final String FINISHED_SELECT_MSQ_QUERY = "QUERY_ID_3";
  private static final String ERRORED_SELECT_MSQ_QUERY = "QUERY_ID_4";

  private static final String RUNNING_NON_MSQ_TASK = "QUERY_ID_5";
  private static final String FAILED_NON_MSQ_TASK = "QUERY_ID_6";
  private static final String FINISHED_NON_MSQ_TASK = "QUERY_ID_7";

  private static final String ACCEPTED_INSERT_MSQ_TASK = "QUERY_ID_8";
  private static final String RUNNING_INSERT_MSQ_QUERY = "QUERY_ID_9";
  private static final String FINISHED_INSERT_MSQ_QUERY = "QUERY_ID_10";
  private static final String ERRORED_INSERT_MSQ_QUERY = "QUERY_ID_11";


  private static final Query<?> QUERY = new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                                                     .intervals(new MultipleIntervalSegmentSpec(
                                                                         Collections.singletonList(Intervals.of(
                                                                             "2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"))))
                                                                     .dataSource("target")
                                                                     .context(ImmutableMap.of(
                                                                         MSQTaskQueryMaker.USER_KEY,
                                                                         AuthConfig.ALLOW_ALL_NAME
                                                                     ))
                                                                     .build();


  private static final MSQControllerTask MSQ_CONTROLLER_SELECT_PAYLOAD = new MSQControllerTask(
      ACCEPTED_SELECT_MSQ_QUERY,
      LegacyMSQSpec.builder()
             .query(QUERY)
             .columnMappings(
                 ColumnMappings.identity(
                     RowSignature.builder()
                                 .add(
                                     "_time",
                                     ColumnType.LONG
                                 )
                                 .add(
                                     "alias",
                                     ColumnType.STRING
                                 )
                                 .add(
                                     "market",
                                     ColumnType.STRING
                                 )
                                 .build()))
             .destination(TaskReportMSQDestination.instance())
             .tuningConfig(
                 MSQTuningConfig.defaultConfig())
             .build(),
      "select _time,alias,market from test",
      new HashMap<>(),
      null,
      ImmutableList.of(
          SqlTypeName.TIMESTAMP,
          SqlTypeName.VARCHAR,
          SqlTypeName.VARCHAR
      ),
      ImmutableList.of(
          ColumnType.LONG,
          ColumnType.STRING,
          ColumnType.STRING
      ),
      null
  );

  private static final MSQControllerTask MSQ_CONTROLLER_INSERT_PAYLOAD = new MSQControllerTask(
      ACCEPTED_SELECT_MSQ_QUERY,
      LegacyMSQSpec.builder()
             .query(QUERY)
             .columnMappings(
                 ColumnMappings.identity(
                     RowSignature.builder()
                                 .add(
                                     "_time",
                                     ColumnType.LONG
                                 )
                                 .add(
                                     "alias",
                                     ColumnType.STRING
                                 )
                                 .add(
                                     "market",
                                     ColumnType.STRING
                                 )
                                 .build()))
             .destination(new DataSourceMSQDestination(
                 "test",
                 Granularities.DAY,
                 null,
                 null,
                 null,
                 null,
                 null
             ))
             .tuningConfig(
                 MSQTuningConfig.defaultConfig())
             .build(),
      "insert into test select _time,alias,market from test",
      new HashMap<>(),
      null,
      ImmutableList.of(
          SqlTypeName.TIMESTAMP,
          SqlTypeName.VARCHAR,
          SqlTypeName.VARCHAR
      ),
      ImmutableList.of(
          ColumnType.LONG,
          ColumnType.STRING,
          ColumnType.STRING
      ),
      null
  );

  private static final List<Object[]> RESULT_ROWS = ImmutableList.of(
      new Object[]{123, "foo", "bar"},
      new Object[]{234, "foo1", "bar1"}
  );

  private final Supplier<MSQTaskReport> selectTaskReport = () -> new MSQTaskReport(
      FINISHED_SELECT_MSQ_QUERY,
      new MSQTaskReportPayload(
          new MSQStatusReport(
              TaskState.SUCCESS,
              null,
              new ArrayDeque<>(),
              null,
              0,
              new HashMap<>(),
              1,
              2,
              null,
              null
          ),
          MSQStagesReport.create(
              MSQTaskReportTest.QUERY_DEFINITION,
              ImmutableMap.of(),
              ImmutableMap.of(),
              ImmutableMap.of(0, 1),
              ImmutableMap.of(0, 1),
              ImmutableMap.of()
          ),
          CounterSnapshotsTree.fromMap(ImmutableMap.of(
              0,
              ImmutableMap.of(
                  0,
                  new CounterSnapshots(ImmutableMap.of(
                      "output",
                      new ChannelCounters.Snapshot(
                          new long[]{1L, 2L},
                          new long[]{3L, 5L},
                          new long[]{},
                          new long[]{},
                          new long[]{}
                      )
                  )
                  )
              )
          )),
          new MSQResultsReport(
              ImmutableList.of(
                  new MSQResultsReport.ColumnAndType(
                      "_time",
                      ColumnType.LONG
                  ),
                  new MSQResultsReport.ColumnAndType(
                      "alias",
                      ColumnType.STRING
                  ),
                  new MSQResultsReport.ColumnAndType(
                      "market",
                      ColumnType.STRING
                  )
              ),
              ImmutableList.of(
                  SqlTypeName.TIMESTAMP,
                  SqlTypeName.VARCHAR,
                  SqlTypeName.VARCHAR
              ),
              RESULT_ROWS,
              null
          )
      )
  );

  private static final MSQTaskReport MSQ_INSERT_TASK_REPORT = new MSQTaskReport(
      FINISHED_INSERT_MSQ_QUERY,
      new MSQTaskReportPayload(
          new MSQStatusReport(
              TaskState.SUCCESS,
              null,
              new ArrayDeque<>(),
              null,
              0,
              new HashMap<>(),
              1,
              2,
              null,
              null
          ),
          MSQStagesReport.create(
              MSQTaskReportTest.QUERY_DEFINITION,
              ImmutableMap.of(),
              ImmutableMap.of(),
              ImmutableMap.of(),
              ImmutableMap.of(),
              ImmutableMap.of()
          ),
          new CounterSnapshotsTree(),
          null
      )
  );
  private static final DateTime QUEUE_INSERTION_TIME = DateTimes.of("2023-05-31T12:01Z");
  public static final ImmutableList<ColumnNameAndTypes> COL_NAME_AND_TYPES = ImmutableList.of(
      new ColumnNameAndTypes(
          "_time",
          SqlTypeName.TIMESTAMP.getName(),
          ValueType.LONG.name()
      ),
      new ColumnNameAndTypes(
          "alias",
          SqlTypeName.VARCHAR.getName(),
          ValueType.STRING.name()
      ),
      new ColumnNameAndTypes(
          "market",
          SqlTypeName.VARCHAR.getName(),
          ValueType.STRING.name()
      )
  );
  private static final String FAILURE_MSG = "failure msg";
  private static SqlStatementResource resource;

  private static final String SUPERUSER = "superuser";
  private static final String STATE_R_USER = "stateR";
  private static final String STATE_W_USER = "stateW";
  private static final String STATE_RW_USER = "stateRW";

  private AuthorizerMapper authorizerMapper = new AuthorizerMapper(null)
  {
    @Override
    public Authorizer getAuthorizer(String name)
    {
      return (authenticationResult, resource, action) -> {
        if (SUPERUSER.equals(authenticationResult.getIdentity())) {
          return Access.OK;
        }

        switch (resource.getType()) {
          case ResourceType.DATASOURCE:
          case ResourceType.VIEW:
          case ResourceType.QUERY_CONTEXT:
          case ResourceType.EXTERNAL:
            return Access.OK;
          case ResourceType.STATE:
            String identity = authenticationResult.getIdentity();
            if (action == Action.READ) {
              if (STATE_R_USER.equals(identity) || STATE_RW_USER.equals(identity)) {
                return Access.OK;
              }
            } else if (action == Action.WRITE) {
              if (STATE_W_USER.equals(identity) || STATE_RW_USER.equals(identity)) {
                return Access.OK;
              }
            }
            return Access.DENIED;

          default:
            return Access.DENIED;
        }
      };
    }
  };

  @Mock
  private OverlordClient overlordClient;

  private void setupMocks(OverlordClient indexingServiceClient)
  {
    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(ACCEPTED_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(ACCEPTED_SELECT_MSQ_QUERY, new TaskStatusPlus(
               ACCEPTED_SELECT_MSQ_QUERY,
               null,
               MSQControllerTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               null,
               null,
               null,
               TaskLocation.unknown(),
               null,
               null
           ))));

    Mockito.when(indexingServiceClient.taskPayload(ArgumentMatchers.eq(ACCEPTED_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskPayloadResponse(
               ACCEPTED_SELECT_MSQ_QUERY,
               MSQ_CONTROLLER_SELECT_PAYLOAD
           )));

    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(RUNNING_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(RUNNING_SELECT_MSQ_QUERY, new TaskStatusPlus(
               RUNNING_SELECT_MSQ_QUERY,
               null,
               MSQControllerTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               TaskState.RUNNING,
               null,
               null,
               TaskLocation.create("test", 0, 0),
               null,
               null
           ))));

    Mockito.when(indexingServiceClient.taskPayload(RUNNING_SELECT_MSQ_QUERY))
           .thenReturn(Futures.immediateFuture(new TaskPayloadResponse(
               RUNNING_SELECT_MSQ_QUERY,
               MSQ_CONTROLLER_SELECT_PAYLOAD
           )));


    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(FINISHED_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(FINISHED_SELECT_MSQ_QUERY, new TaskStatusPlus(
               FINISHED_SELECT_MSQ_QUERY,
               null,
               MSQControllerTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               TaskState.SUCCESS,
               null,
               100L,
               TaskLocation.create("test", 0, 0),
               null,
               null
           ))));

    Mockito.when(indexingServiceClient.taskPayload(FINISHED_SELECT_MSQ_QUERY))
           .thenReturn(Futures.immediateFuture(new TaskPayloadResponse(
               FINISHED_SELECT_MSQ_QUERY,
               MSQ_CONTROLLER_SELECT_PAYLOAD
           )));


    Mockito.when(indexingServiceClient.taskReportAsMap(ArgumentMatchers.eq(FINISHED_SELECT_MSQ_QUERY)))
           .thenAnswer(inv -> Futures.immediateFuture(TaskReport.buildTaskReports(selectTaskReport.get())));
    Mockito.when(indexingServiceClient.taskReportAsMap(ArgumentMatchers.eq(ACCEPTED_SELECT_MSQ_QUERY)))
           .thenAnswer(inv -> Futures.immediateFuture(TaskReport.buildTaskReports(selectTaskReport.get())));
    Mockito.when(indexingServiceClient.taskReportAsMap(ArgumentMatchers.eq(RUNNING_SELECT_MSQ_QUERY)))
           .thenAnswer(inv -> Futures.immediateFuture(TaskReport.buildTaskReports(selectTaskReport.get())));

    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(ERRORED_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(ERRORED_SELECT_MSQ_QUERY, new TaskStatusPlus(
               ERRORED_SELECT_MSQ_QUERY,
               null,
               MSQControllerTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               TaskState.FAILED,
               null,
               -1L,
               TaskLocation.unknown(),
               null,
               FAILURE_MSG
           ))));

    Mockito.when(indexingServiceClient.taskPayload(ArgumentMatchers.eq(ERRORED_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskPayloadResponse(
               FINISHED_INSERT_MSQ_QUERY,
               MSQ_CONTROLLER_SELECT_PAYLOAD
           )));

    Mockito.when(indexingServiceClient.taskReportAsMap(ArgumentMatchers.eq(ERRORED_SELECT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(null));

    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(RUNNING_NON_MSQ_TASK)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(RUNNING_NON_MSQ_TASK, new TaskStatusPlus(
               RUNNING_NON_MSQ_TASK,
               null,
               null,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               TaskState.RUNNING,
               null,
               -1L,
               TaskLocation.unknown(),
               null,
               null
           ))));


    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(FAILED_NON_MSQ_TASK)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(FAILED_NON_MSQ_TASK, new TaskStatusPlus(
               FAILED_NON_MSQ_TASK,
               null,
               null,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               TaskState.FAILED,
               null,
               -1L,
               TaskLocation.unknown(),
               null,
               FAILURE_MSG
           ))));


    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(FINISHED_NON_MSQ_TASK)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(FINISHED_NON_MSQ_TASK, new TaskStatusPlus(
               FINISHED_NON_MSQ_TASK,
               null,
               IndexTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               TaskState.SUCCESS,
               null,
               -1L,
               TaskLocation.unknown(),
               null,
               null
           ))));


    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(ACCEPTED_INSERT_MSQ_TASK)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(ACCEPTED_SELECT_MSQ_QUERY, new TaskStatusPlus(
               ACCEPTED_SELECT_MSQ_QUERY,
               null,
               MSQControllerTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               null,
               null,
               null,
               TaskLocation.unknown(),
               null,
               null
           ))));

    Mockito.when(indexingServiceClient.taskPayload(ArgumentMatchers.eq(ACCEPTED_INSERT_MSQ_TASK)))
           .thenReturn(Futures.immediateFuture(new TaskPayloadResponse(
               ACCEPTED_INSERT_MSQ_TASK,
               MSQ_CONTROLLER_INSERT_PAYLOAD
           )));

    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(RUNNING_INSERT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(RUNNING_INSERT_MSQ_QUERY, new TaskStatusPlus(
               RUNNING_INSERT_MSQ_QUERY,
               null,
               MSQControllerTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               TaskState.RUNNING,
               null,
               null,
               TaskLocation.create("test", 0, 0),
               null,
               null
           ))));

    Mockito.when(indexingServiceClient.taskPayload(RUNNING_INSERT_MSQ_QUERY))
           .thenReturn(Futures.immediateFuture(new TaskPayloadResponse(
               RUNNING_INSERT_MSQ_QUERY,
               MSQ_CONTROLLER_INSERT_PAYLOAD
           )));


    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(FINISHED_INSERT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(FINISHED_INSERT_MSQ_QUERY, new TaskStatusPlus(
               FINISHED_INSERT_MSQ_QUERY,
               null,
               MSQControllerTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               TaskState.SUCCESS,
               null,
               100L,
               TaskLocation.create("test", 0, 0),
               null,
               null
           ))));

    Mockito.when(indexingServiceClient.taskReportAsMap(ArgumentMatchers.eq(FINISHED_INSERT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(TaskReport.buildTaskReports(MSQ_INSERT_TASK_REPORT)));
    Mockito.when(indexingServiceClient.taskReportAsMap(ArgumentMatchers.eq(ACCEPTED_INSERT_MSQ_TASK)))
           .thenReturn(Futures.immediateFuture(TaskReport.buildTaskReports(MSQ_INSERT_TASK_REPORT)));
    Mockito.when(indexingServiceClient.taskReportAsMap(ArgumentMatchers.eq(RUNNING_INSERT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(TaskReport.buildTaskReports(MSQ_INSERT_TASK_REPORT)));

    Mockito.when(indexingServiceClient.taskPayload(FINISHED_INSERT_MSQ_QUERY))
           .thenReturn(Futures.immediateFuture(new TaskPayloadResponse(
               FINISHED_INSERT_MSQ_QUERY,
               MSQ_CONTROLLER_INSERT_PAYLOAD
           )));

    Mockito.when(indexingServiceClient.taskStatus(ArgumentMatchers.eq(ERRORED_INSERT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(ERRORED_INSERT_MSQ_QUERY, new TaskStatusPlus(
               ERRORED_INSERT_MSQ_QUERY,
               null,
               MSQControllerTask.TYPE,
               CREATED_TIME,
               QUEUE_INSERTION_TIME,
               TaskState.FAILED,
               null,
               -1L,
               TaskLocation.unknown(),
               null,
               FAILURE_MSG
           ))));

    Mockito.when(indexingServiceClient.taskPayload(ArgumentMatchers.eq(ERRORED_INSERT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(new TaskPayloadResponse(
               ERRORED_INSERT_MSQ_QUERY,
               MSQ_CONTROLLER_INSERT_PAYLOAD
           )));

    Mockito.when(indexingServiceClient.taskReportAsMap(ArgumentMatchers.eq(ERRORED_INSERT_MSQ_QUERY)))
           .thenReturn(Futures.immediateFuture(null));

  }

  public static void assertNotFound(Response response, String queryId)
  {
    assertExceptionMessage(response, StringUtils.format("Query [%s] was not found. The query details are no longer present or might not be of the type [%s]. Verify that the id is correct.", queryId, MSQControllerTask.TYPE), Response.Status.NOT_FOUND);
  }

  public static void assertExceptionMessage(
      Response response,
      String exceptionMessage,
      Response.Status expectectedStatus
  )
  {
    Assert.assertEquals(expectectedStatus.getStatusCode(), response.getStatus());
    Assert.assertEquals(exceptionMessage, getQueryExceptionFromResponse(response));
  }

  public static List getResultRowsFromResponse(Response resultsResponse) throws IOException
  {
    byte[] bytes = SqlResourceTest.responseToByteArray(resultsResponse);
    if (bytes == null) {
      return null;
    }
    return JSON_MAPPER.readValue(bytes, List.class);
  }

  private static String getQueryExceptionFromResponse(Response response)
  {
    if (response.getEntity() instanceof SqlStatementResult) {
      return ((SqlStatementResult) response.getEntity()).getErrorResponse().getUnderlyingException().getMessage();
    } else {
      return ((ErrorResponse) response.getEntity()).getUnderlyingException().getMessage();
    }
  }

  public static MockHttpServletRequest makeOkRequest()
  {
    return makeExpectedReq(CalciteTests.REGULAR_USER_AUTH_RESULT);
  }

  private static MockHttpServletRequest makeExpectedReq(AuthenticationResult authenticationResult)
  {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
    req.remoteAddr = "1.2.3.4";
    return req;
  }

  private static AuthenticationResult makeAuthResultForUser(String user)
  {
    return new AuthenticationResult(
        user,
        AuthConfig.ALLOW_ALL_NAME,
        null,
        null
    );
  }

  @Nullable
  private Object getHeader(Response resp, String header)
  {
    final List<Object> objects = resp.getMetadata().get(header);
    if (objects == null) {
      return null;
    }
    return Iterables.getOnlyElement(objects);
  }

  @BeforeEach
  public void init()
  {
    overlordClient = Mockito.mock(OverlordClient.class);
    setupMocks(overlordClient);
    resource = new SqlStatementResource(
        sqlStatementFactory,
        objectMapper,
        overlordClient,
        tempDir -> localFileStorageConnector,
        authorizerMapper,
        DefaultQueryConfig.NIL
    );
  }

  @Test
  public void testMSQSelectAcceptedQuery()
  {
    Response response = resource.doGetStatus(ACCEPTED_SELECT_MSQ_QUERY, false, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertSqlStatementResult(
        new SqlStatementResult(
            ACCEPTED_SELECT_MSQ_QUERY,
            SqlStatementState.ACCEPTED,
            CREATED_TIME,
            COL_NAME_AND_TYPES,
            null,
            null,
            null
        ),
        (SqlStatementResult) response.getEntity()
    );

    assertExceptionMessage(
        resource.doGetResults(ACCEPTED_SELECT_MSQ_QUERY, 0L, null, null, makeOkRequest()),
        StringUtils.format(
            "Query[%s] is currently in [%s] state. Please wait for it to complete.",
            ACCEPTED_SELECT_MSQ_QUERY,
            SqlStatementState.ACCEPTED
        ),
        Response.Status.BAD_REQUEST
    );
    Assert.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(ACCEPTED_SELECT_MSQ_QUERY, makeOkRequest()).getStatus()
    );
  }

  @Test
  public void testMSQSelectRunningQuery()
  {

    Response response = resource.doGetStatus(RUNNING_SELECT_MSQ_QUERY, false, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertSqlStatementResult(
        new SqlStatementResult(
            RUNNING_SELECT_MSQ_QUERY,
            SqlStatementState.RUNNING,
            CREATED_TIME,
            COL_NAME_AND_TYPES,
            null,
            null,
            null
        ),
        (SqlStatementResult) response.getEntity()
    );

    assertExceptionMessage(
        resource.doGetResults(RUNNING_SELECT_MSQ_QUERY, 0L, null, null, makeOkRequest()),
        StringUtils.format(
            "Query[%s] is currently in [%s] state. Please wait for it to complete.",
            RUNNING_SELECT_MSQ_QUERY,
            SqlStatementState.RUNNING
        ),
        Response.Status.BAD_REQUEST
    );
    Assert.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(RUNNING_SELECT_MSQ_QUERY, makeOkRequest()).getStatus()
    );
  }

  @Test
  public void testMSQSelectRunningQueryWithDetail()
  {
    Response response = resource.doGetStatus(RUNNING_SELECT_MSQ_QUERY, true, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    SqlStatementResult expectedSqlStatementResult = new SqlStatementResult(
        RUNNING_SELECT_MSQ_QUERY,
        SqlStatementState.RUNNING,
        CREATED_TIME,
        COL_NAME_AND_TYPES,
        null,
        null,
        null,
        selectTaskReport.get().getPayload().getStages(),
        selectTaskReport.get().getPayload().getCounters(),
        new ArrayList<>(selectTaskReport.get().getPayload().getStatus().getWarningReports())
    );

    assertSqlStatementResult(
        expectedSqlStatementResult,
        (SqlStatementResult) response.getEntity()
    );

    Assert.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(RUNNING_SELECT_MSQ_QUERY, makeOkRequest()).getStatus()
    );
  }

  @Test
  public void testFinishedSelectMSQQuery() throws Exception
  {
    Response response = resource.doGetStatus(FINISHED_SELECT_MSQ_QUERY, false, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(objectMapper.writeValueAsString(new SqlStatementResult(
        FINISHED_SELECT_MSQ_QUERY,
        SqlStatementState.SUCCESS,
        CREATED_TIME,
        COL_NAME_AND_TYPES,
        100L,
        new ResultSetInformation(
            3L,
            8L,
            null,
            MSQControllerTask.DUMMY_DATASOURCE_FOR_SELECT,
            RESULT_ROWS,
            ImmutableList.of(new PageInformation(0, 3L, 8L))
        ),
        null
    )), objectMapper.writeValueAsString(response.getEntity()));

    Response resultsResponse = resource.doGetResults(FINISHED_SELECT_MSQ_QUERY, 0L, ResultFormat.OBJECTLINES.name(), null, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), resultsResponse.getStatus());

    String expectedResult = "{\"_time\":123,\"alias\":\"foo\",\"market\":\"bar\"}\n"
                            + "{\"_time\":234,\"alias\":\"foo1\",\"market\":\"bar1\"}\n\n";

    assertExpectedResults(expectedResult, resultsResponse);

    Assert.assertNull(getHeader(resultsResponse, SqlStatementResource.CONTENT_DISPOSITION_RESPONSE_HEADER));

    Assert.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.deleteQuery(FINISHED_SELECT_MSQ_QUERY, makeOkRequest()).getStatus()
    );

    assertExpectedResults(
        expectedResult,
        resource.doGetResults(
            FINISHED_SELECT_MSQ_QUERY,
            0L,
            ResultFormat.OBJECTLINES.name(),
            null,
            makeOkRequest()
        )
    );

    assertExpectedResults(
        expectedResult,
        resource.doGetResults(
            FINISHED_SELECT_MSQ_QUERY,
            null,
            ResultFormat.OBJECTLINES.name(),
            null,
            makeOkRequest()
        )
    );

    Assert.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        resource.doGetResults(FINISHED_SELECT_MSQ_QUERY, -1L, null, null, makeOkRequest()).getStatus()
    );

    Assert.assertEquals(
        "attachment; filename=\"my-file.ndjson\"",
        getHeader(
            resource.doGetResults(FINISHED_SELECT_MSQ_QUERY, 0L, ResultFormat.OBJECTLINES.name(), "my-file.ndjson", makeOkRequest()),
            SqlStatementResource.CONTENT_DISPOSITION_RESPONSE_HEADER
        )
    );
  }

  @Test
  public void testDownloadResultsAsFile() throws Exception
  {
    final String expectedResult = "{\"_time\":123,\"alias\":\"foo\",\"market\":\"bar\"}\n"
                                  + "{\"_time\":234,\"alias\":\"foo1\",\"market\":\"bar1\"}\n\n";

    Response resultsResponse1 = resource.doGetResults(FINISHED_SELECT_MSQ_QUERY, 0L, ResultFormat.OBJECTLINES.name(), "results.txt", makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), resultsResponse1.getStatus());
    Assert.assertEquals(
        "attachment; filename=\"results.txt\"",
        getHeader(resultsResponse1, "Content-Disposition")
    );
    assertExpectedResults(expectedResult, resultsResponse1);

    Response resultsResponse2 = resource.doGetResults(FINISHED_SELECT_MSQ_QUERY, 0L, ResultFormat.OBJECTLINES.name(), "final results.txt", makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), resultsResponse2.getStatus());
    Assert.assertEquals(
        "attachment; filename=\"final results.txt\"",
        getHeader(resultsResponse2, "Content-Disposition")
    );
    assertExpectedResults(expectedResult, resultsResponse2);

    Response resultsResponse3 = resource.doGetResults(FINISHED_SELECT_MSQ_QUERY, 0L, ResultFormat.OBJECTLINES.name(), "/Users/Name/final.txt", makeOkRequest());
    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resultsResponse3.getStatus());
    Assert.assertNull(resultsResponse3.getMetadata().get("Content-Disposition"));
  }

  private void assertExpectedResults(String expectedResult, Response resultsResponse) throws IOException
  {
    byte[] bytes = SqlResourceTest.responseToByteArray(resultsResponse);
    Assert.assertEquals(expectedResult, new String(bytes, StandardCharsets.UTF_8));
  }

  @Test
  public void testFailedMSQQuery()
  {
    for (String queryID : ImmutableList.of(ERRORED_SELECT_MSQ_QUERY, ERRORED_INSERT_MSQ_QUERY)) {
      assertExceptionMessage(resource.doGetStatus(queryID, false, makeOkRequest()), FAILURE_MSG, Response.Status.OK);
      assertExceptionMessage(
          resource.doGetResults(queryID, 0L, null, null, makeOkRequest()),
          StringUtils.format(
              "Query[%s] failed. Check the status api for more details.",
              queryID
          ),
          Response.Status.BAD_REQUEST
      );

      Assert.assertEquals(
          Response.Status.OK.getStatusCode(),
          resource.deleteQuery(queryID, makeOkRequest()).getStatus()
      );
    }
  }

  @Test
  public void testFinishedInsertMSQQuery()
  {
    Response response = resource.doGetStatus(FINISHED_INSERT_MSQ_QUERY, false, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertSqlStatementResult(new SqlStatementResult(
        FINISHED_INSERT_MSQ_QUERY,
        SqlStatementState.SUCCESS,
        CREATED_TIME,
        null,
        100L,
        new ResultSetInformation(null, null, null, "test", null, null),
        null
    ), (SqlStatementResult) response.getEntity());

    final Response resultResponse = resource.doGetResults(FINISHED_INSERT_MSQ_QUERY, 0L, null, null, makeOkRequest());

    Assert.assertEquals(
        Response.Status.OK.getStatusCode(),
        resultResponse.getStatus()
    );
    Assert.assertNull(getHeader(resultResponse, SqlStatementResource.CONTENT_DISPOSITION_RESPONSE_HEADER));
    Assert.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.doGetResults(FINISHED_INSERT_MSQ_QUERY, null, null, null, makeOkRequest()).getStatus()
    );

    Assert.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        resource.doGetResults(FINISHED_INSERT_MSQ_QUERY, -1L, null, null, makeOkRequest()).getStatus()
    );

    Assert.assertEquals(
        "attachment; filename=\"my-file.ndjson\"",
        getHeader(
            resource.doGetResults(FINISHED_INSERT_MSQ_QUERY, 0L, null, "my-file.ndjson", makeOkRequest()),
            SqlStatementResource.CONTENT_DISPOSITION_RESPONSE_HEADER
        )
    );
  }

  @Test
  public void testNonMSQTasks()
  {
    for (String queryID : ImmutableList.of(RUNNING_NON_MSQ_TASK, FAILED_NON_MSQ_TASK, FINISHED_NON_MSQ_TASK)) {
      assertNotFound(resource.doGetStatus(queryID, false, makeOkRequest()), queryID);
      assertNotFound(resource.doGetResults(queryID, 0L, null, null, makeOkRequest()), queryID);
      assertNotFound(resource.deleteQuery(queryID, makeOkRequest()), queryID);
    }
  }

  @Test
  public void testMSQInsertAcceptedQuery()
  {
    Response response = resource.doGetStatus(ACCEPTED_INSERT_MSQ_TASK, false, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertSqlStatementResult(
        new SqlStatementResult(
            ACCEPTED_INSERT_MSQ_TASK,
            SqlStatementState.ACCEPTED,
            CREATED_TIME,
            null,
            null,
            null,
            null
        ),
        (SqlStatementResult) response.getEntity()
    );

    assertExceptionMessage(
        resource.doGetResults(ACCEPTED_INSERT_MSQ_TASK, 0L, null, null, makeOkRequest()),
        StringUtils.format(
            "Query[%s] is currently in [%s] state. Please wait for it to complete.",
            ACCEPTED_INSERT_MSQ_TASK,
            SqlStatementState.ACCEPTED
        ),
        Response.Status.BAD_REQUEST
    );
    Assert.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(ACCEPTED_INSERT_MSQ_TASK, makeOkRequest()).getStatus()
    );
  }

  @Test
  public void testMSQInsertRunningQuery()
  {
    Response response = resource.doGetStatus(RUNNING_INSERT_MSQ_QUERY, false, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertSqlStatementResult(
        new SqlStatementResult(
            RUNNING_INSERT_MSQ_QUERY,
            SqlStatementState.RUNNING,
            CREATED_TIME,
            null,
            null,
            null,
            null
        ),
        (SqlStatementResult) response.getEntity()
    );

    assertExceptionMessage(
        resource.doGetResults(RUNNING_INSERT_MSQ_QUERY, 0L, null, null, makeOkRequest()),
        StringUtils.format(
            "Query[%s] is currently in [%s] state. Please wait for it to complete.",
            RUNNING_INSERT_MSQ_QUERY,
            SqlStatementState.RUNNING
        ),
        Response.Status.BAD_REQUEST
    );
    Assert.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(RUNNING_INSERT_MSQ_QUERY, makeOkRequest()).getStatus()
    );
  }

  @Test
  public void testAPIBehaviourWithSuperUsers()
  {
    Assert.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.doGetStatus(
            RUNNING_SELECT_MSQ_QUERY,
            false,
            makeExpectedReq(makeAuthResultForUser(SUPERUSER))
        ).getStatus()
    );
    Assert.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        resource.doGetResults(
            RUNNING_SELECT_MSQ_QUERY,
            1L,
            null,
            null,
            makeExpectedReq(makeAuthResultForUser(SUPERUSER))
        ).getStatus()
    );
    Assert.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(
            RUNNING_SELECT_MSQ_QUERY,
            makeExpectedReq(makeAuthResultForUser(SUPERUSER))
        ).getStatus()
    );
  }

  @Test
  public void testAPIBehaviourWithDifferentUserAndNoStatePermission()
  {
    AuthenticationResult differentUserAuthResult = makeAuthResultForUser("differentUser");
    Assert.assertEquals(
        Response.Status.FORBIDDEN.getStatusCode(),
        resource.doGetStatus(
            RUNNING_SELECT_MSQ_QUERY,
            false,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assert.assertEquals(
        Response.Status.FORBIDDEN.getStatusCode(),
        resource.doGetResults(
            RUNNING_SELECT_MSQ_QUERY,
            1L,
            null,
            null,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assert.assertEquals(
        Response.Status.FORBIDDEN.getStatusCode(),
        resource.deleteQuery(
            RUNNING_SELECT_MSQ_QUERY,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
  }

  @Test
  public void testAPIBehaviourWithDifferentUserAndStateRPermission()
  {
    AuthenticationResult differentUserAuthResult = makeAuthResultForUser(STATE_R_USER);
    Assert.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.doGetStatus(
            RUNNING_SELECT_MSQ_QUERY,
            false,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assert.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        resource.doGetResults(
            RUNNING_SELECT_MSQ_QUERY,
            1L,
            null,
            null,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assert.assertEquals(
        Response.Status.FORBIDDEN.getStatusCode(),
        resource.deleteQuery(
            RUNNING_SELECT_MSQ_QUERY,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
  }

  @Test
  public void testAPIBehaviourWithDifferentUserAndStateWPermission()
  {
    AuthenticationResult differentUserAuthResult = makeAuthResultForUser(STATE_W_USER);
    Assert.assertEquals(
        Response.Status.FORBIDDEN.getStatusCode(),
        resource.doGetStatus(
            RUNNING_SELECT_MSQ_QUERY,
            false,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assert.assertEquals(
        Response.Status.FORBIDDEN.getStatusCode(),
        resource.doGetResults(
            RUNNING_SELECT_MSQ_QUERY,
            1L,
            null,
            null,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assert.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(
            RUNNING_SELECT_MSQ_QUERY,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
  }

  @Test
  public void testAPIBehaviourWithDifferentUserAndStateRWPermission()
  {
    AuthenticationResult differentUserAuthResult = makeAuthResultForUser(STATE_RW_USER);
    Assert.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.doGetStatus(
            RUNNING_SELECT_MSQ_QUERY,
            false,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assert.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        resource.doGetResults(
            RUNNING_SELECT_MSQ_QUERY,
            1L,
            null,
            null,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
    Assert.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(
            RUNNING_SELECT_MSQ_QUERY,
            makeExpectedReq(differentUserAuthResult)
        ).getStatus()
    );
  }

  @Test
  public void testTaskIdNotFound()
  {
    String taskIdNotFound = "notFound";
    final DefaultHttpResponse incorrectResponse =
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
    SettableFuture<TaskStatusResponse> settableFuture = SettableFuture.create();
    settableFuture.setException(new HttpResponseException(new StringFullResponseHolder(
        incorrectResponse,
        StandardCharsets.UTF_8
    )));
    Mockito.when(overlordClient.taskStatus(taskIdNotFound)).thenReturn(settableFuture);

    Assert.assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        resource.doGetStatus(taskIdNotFound, false, makeOkRequest()).getStatus()
    );
    Assert.assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        resource.doGetResults(taskIdNotFound, null, null, null, makeOkRequest()).getStatus()
    );
    Assert.assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        resource.deleteQuery(taskIdNotFound, makeOkRequest()).getStatus()
    );
  }

  @Test
  public void testIsEnabled()
  {
    Assert.assertEquals(Response.Status.OK.getStatusCode(), resource.isEnabled(makeOkRequest()).getStatus());
  }

  private void assertSqlStatementResult(SqlStatementResult expected, SqlStatementResult actual)
  {
    Assert.assertEquals(expected.getQueryId(), actual.getQueryId());
    Assert.assertEquals(expected.getCreatedAt(), actual.getCreatedAt());
    Assert.assertEquals(expected.getSqlRowSignature(), actual.getSqlRowSignature());
    Assert.assertEquals(expected.getDurationMs(), actual.getDurationMs());
    Assert.assertEquals(expected.getStages(), actual.getStages());
    Assert.assertEquals(expected.getState(), actual.getState());
    Assert.assertEquals(expected.getWarnings(), actual.getWarnings());
    Assert.assertEquals(expected.getResultSetInformation(), actual.getResultSetInformation());

    if (actual.getCounters() == null || expected.getCounters() == null) {
      Assert.assertEquals(expected.getCounters(), actual.getCounters());
    } else {
      Assert.assertEquals(expected.getCounters().toString(), actual.getCounters().toString());
    }

    if (actual.getErrorResponse() == null || expected.getErrorResponse() == null) {
      Assert.assertEquals(expected.getErrorResponse(), actual.getErrorResponse());
    } else {
      Assert.assertEquals(expected.getErrorResponse().getAsMap(), actual.getErrorResponse().getAsMap());
    }
  }

  @Test
  public void testValidFilename()
  {
    // Valid cases
    SqlStatementResource.validateFilename("testname");
    SqlStatementResource.validateFilename("A.txt");
    SqlStatementResource.validateFilename("final-results.txt");
    SqlStatementResource.validateFilename("final results.txt");
    SqlStatementResource.validateFilename("final;results.txt");
    SqlStatementResource.validateFilename("final@results.txt");

    // Empty
    assertInvalidFileName("", "Filename cannot be empty.");

    // Too long
    assertInvalidFileName(StringUtils.repeat("A", 300), "Filename cannot be longer than 255 characters.");

    // Special characters
    assertInvalidFileName("He\\llo", "Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
    assertInvalidFileName("Hello/Name", "Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
    assertInvalidFileName("C:/Users/Name", "Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
    assertInvalidFileName("username:password", "Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
    assertInvalidFileName("A>ValueB", "Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
    assertInvalidFileName("A<ValueB", "Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
    assertInvalidFileName("A\0a11", "Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
    assertInvalidFileName("\rrB", "Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
    assertInvalidFileName("\nAnB", "Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
    assertInvalidFileName("A|B", "Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
    assertInvalidFileName("A\"B", "Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
    assertInvalidFileName("A?B", "Filename contains invalid characters. (/, \\, :, *, ?, \", <, >, |, \0, \n, or \r)");
  }

  private void assertInvalidFileName(String filename, String errorMessage)
  {
    assertThat(
        Assert.assertThrows(DruidException.class, () -> SqlStatementResource.validateFilename(filename)),
        DruidExceptionMatcher.invalidInput().expectMessageIs(errorMessage)
    );
  }
}
