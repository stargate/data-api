package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.junit.jupiter.api.Assertions.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.api.response.BeginOfflineSessionResponse;
import io.stargate.sgv2.jsonapi.api.response.OfflineGetStatusResponse;
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

public class OfflineGetStatusOperationTest {

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
  public void testOfflineGetStatusOperationInvalidId() {
    String sessionId = "invalid-id";
    OfflineGetStatusOperation offlineGetStatusOperation = new OfflineGetStatusOperation(sessionId);
    OperationsConfig operationsConfig = OfflineFileWriterInitializer.buildOperationsConfig();
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(operationsConfig, new SimpleMeterRegistry());
    QueryExecutor queryExecutor = new QueryExecutor(cqlSessionCache, operationsConfig);
    OfflineGetStatusResponse offlineGetStatusResponse =
        OfflineGetStatusResponse.fromCommandResult(
            offlineGetStatusOperation
                .execute(new DataApiRequestInfo(Optional.of(sessionId)), queryExecutor)
                .subscribe()
                .withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem()
                .get());
    assertNull(offlineGetStatusResponse.offlineWriterSessionStatus());
    assertNotNull(offlineGetStatusResponse.errors());
    assertEquals(1, offlineGetStatusResponse.errors().size());
    assertEquals(
        "Offline writer session not found", offlineGetStatusResponse.errors().get(0).message());
    assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(),
        offlineGetStatusResponse.errors().get(0).status().getStatusCode());
  }

  @Test
  public void testOfflineGetStatusOperation() {
    String sessionId = UUID.randomUUID().toString();
    OperationsConfig operationsConfig = OfflineFileWriterInitializer.buildOperationsConfig();
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(operationsConfig, new SimpleMeterRegistry());
    QueryExecutor queryExecutor = new QueryExecutor(cqlSessionCache, operationsConfig);
    beginASession(sessionId, cqlSessionCache, queryExecutor);
    OfflineGetStatusOperation offlineGetStatusOperation = new OfflineGetStatusOperation(sessionId);
    OfflineGetStatusResponse offlineGetStatusResponse =
        OfflineGetStatusResponse.fromCommandResult(
            offlineGetStatusOperation
                .execute(new DataApiRequestInfo(Optional.of(sessionId)), queryExecutor)
                .subscribe()
                .withSubscriber(UniAssertSubscriber.create())
                .awaitItem()
                .getItem()
                .get());
    assertNotNull(offlineGetStatusResponse.offlineWriterSessionStatus());
    assertEquals("testnamespace", offlineGetStatusResponse.offlineWriterSessionStatus().keyspace());
    assertEquals(
        "testcollection", offlineGetStatusResponse.offlineWriterSessionStatus().tableName());
    assertEquals(
        SSTAbleOutputDirectory,
        offlineGetStatusResponse.offlineWriterSessionStatus().ssTableOutputDirectory());
    assertEquals(
        20, offlineGetStatusResponse.offlineWriterSessionStatus().fileWriterBufferSizeInMB());
    assertEquals(
        0, offlineGetStatusResponse.offlineWriterSessionStatus().dataDirectorySizeInBytes());
    assertEquals(sessionId, offlineGetStatusResponse.offlineWriterSessionStatus().sessionId());
    assertEquals(0, offlineGetStatusResponse.offlineWriterSessionStatus().insertsFailed());
    assertEquals(0, offlineGetStatusResponse.offlineWriterSessionStatus().insertsSucceeded());
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
