package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.junit.jupiter.api.Assertions.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.api.response.BeginOfflineSessionResponse;
import io.stargate.sgv2.jsonapi.api.response.EndOfflineSessionResponse;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.OfflineFileWriterInitializer;
import jakarta.ws.rs.core.Response;
import java.io.File;
import java.util.Optional;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class EndOfflineSessionOperationTest {

  private static final String SSTAbleOutputDirectory =
      System.getProperty("java.io.tmpdir") + "sstables_test";

  @BeforeAll
  public static void setup() {
    OfflineFileWriterInitializer.initialize();
    File sstablesDirectory = new File(SSTAbleOutputDirectory);
    if (!sstablesDirectory.exists()) {
      if (!sstablesDirectory.mkdirs()) {
        throw new RuntimeException("Failed to create sstables directory");
      }
    }
  }

  @AfterAll
  public static void cleanup() {
    File sstablesDirectory = new File(SSTAbleOutputDirectory);
    if (sstablesDirectory.exists()) {
      if (!sstablesDirectory.delete()) {
        throw new RuntimeException("Failed to delete sstables directory");
      }
    }
  }

  @Test
  public void testEndOfflineSessionOperationInvalidId() {
    String sessionId = "invalid-id";
    EndOfflineSessionOperation endOfflineSessionOperation =
        new EndOfflineSessionOperation(sessionId);
    OperationsConfig operationsConfig = OfflineFileWriterInitializer.buildOperationsConfig();
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(operationsConfig, new SimpleMeterRegistry());
    QueryExecutor queryExecutor = new QueryExecutor(cqlSessionCache, operationsConfig);
    EndOfflineSessionResponse endOfflineSessionResponse =
        EndOfflineSessionResponse.fromCommandResult(
            endOfflineSessionOperation
                .execute(new DataApiRequestInfo(Optional.of(sessionId)), queryExecutor)
                .subscribe()
                .withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem()
                .get());
    assertNull(endOfflineSessionResponse.offlineWriterSessionStatus());
    assertNotNull(endOfflineSessionResponse.errors());
    assertEquals(1, endOfflineSessionResponse.errors().size());
    assertEquals("Session not found", endOfflineSessionResponse.errors().get(0).message());
    assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        endOfflineSessionResponse.errors().get(0).status().getStatusCode());
  }

  @Test
  public void testEndOfflineSessionOperation() {
    String sessionId = UUID.randomUUID().toString();
    OperationsConfig operationsConfig = OfflineFileWriterInitializer.buildOperationsConfig();
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(operationsConfig, new SimpleMeterRegistry());
    QueryExecutor queryExecutor = new QueryExecutor(cqlSessionCache, operationsConfig);
    beginASession(sessionId, cqlSessionCache, queryExecutor);
    EndOfflineSessionOperation endOfflineSessionOperation =
        new EndOfflineSessionOperation(sessionId);
    EndOfflineSessionResponse endOfflineSessionResponse =
        EndOfflineSessionResponse.fromCommandResult(
            endOfflineSessionOperation
                .execute(new DataApiRequestInfo(Optional.of(sessionId)), queryExecutor)
                .subscribe()
                .withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem()
                .get());
    assertNotNull(endOfflineSessionResponse.offlineWriterSessionStatus());
    assertEquals(
        "testnamespace", endOfflineSessionResponse.offlineWriterSessionStatus().keyspace());
    assertEquals(
        "testcollection", endOfflineSessionResponse.offlineWriterSessionStatus().tableName());
    assertEquals(
        SSTAbleOutputDirectory,
        endOfflineSessionResponse.offlineWriterSessionStatus().ssTableOutputDirectory());
    assertEquals(
        20, endOfflineSessionResponse.offlineWriterSessionStatus().fileWriterBufferSizeInMB());
    assertEquals(
        0, endOfflineSessionResponse.offlineWriterSessionStatus().dataDirectorySizeInBytes());
    assertEquals(sessionId, endOfflineSessionResponse.offlineWriterSessionStatus().sessionId());
    assertEquals(0, endOfflineSessionResponse.offlineWriterSessionStatus().insertsFailed());
    assertEquals(0, endOfflineSessionResponse.offlineWriterSessionStatus().insertsSucceeded());
  }

  private void beginASession(
      String sessionId, CQLSessionCache cqlSessionCache, QueryExecutor queryExecutor) {
    BeginOfflineSessionOperation beginOfflineSessionOperation =
        getBeginOfflineSessionOperation(sessionId);
    DataApiRequestInfo dataApiRequestInfo = new DataApiRequestInfo(Optional.of(sessionId));
    CommandResult commandResult =
        beginOfflineSessionOperation
            .execute(dataApiRequestInfo, queryExecutor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem()
            .get();
    BeginOfflineSessionResponse beginOfflineSessionResponse =
        BeginOfflineSessionResponse.fromCommandResult(commandResult);
    assertEquals(sessionId, beginOfflineSessionResponse.sessionId());
    assertNotNull(cqlSessionCache.getSession(dataApiRequestInfo));
  }

  private static @NotNull BeginOfflineSessionOperation getBeginOfflineSessionOperation(
      String sessionId) {
    String createTableCQL =
        "create table testnamespace.testcollection (id uuid primary key, value text);";
    String insertStatementCQL =
        "insert into testnamespace.testcollection (id, value) values (?, ?);";
    FileWriterParams fileWriterParams =
        new FileWriterParams(
            "testnamespace",
            "testcollection",
            SSTAbleOutputDirectory,
            20,
            createTableCQL,
            insertStatementCQL,
            null,
            false);
    return new BeginOfflineSessionOperation(sessionId, fileWriterParams);
  }
}
