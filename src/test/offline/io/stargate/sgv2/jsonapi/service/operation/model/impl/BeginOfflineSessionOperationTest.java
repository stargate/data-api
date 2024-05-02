package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.api.response.BeginOfflineSessionResponse;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.OfflineFileWriterInitializer;
import java.io.File;
import java.util.Optional;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeginOfflineSessionOperationTest {
  private static final Logger logger =
      LoggerFactory.getLogger(BeginOfflineSessionOperationTest.class);
  private static final String SSTAbleOutputDirectory =
      System.getProperty("java.io.tmpdir") + File.separator + "sstables_test";

  @BeforeAll
  public static void setup() {
    OfflineFileWriterInitializer.initialize();
    File sstablesDirectory = new File(SSTAbleOutputDirectory);
    logger.info("Trying to create sstables directory if not exists: {}", SSTAbleOutputDirectory);
    if (!sstablesDirectory.exists()) {
      logger.info("Directory {} doesn't exist, creating", SSTAbleOutputDirectory);
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
  public void testBeginOfflineSessionOperation() {
    String sessionId = UUID.randomUUID().toString();
    BeginOfflineSessionOperation beginOfflineSessionOperation =
        getBeginOfflineSessionOperation(sessionId);
    DataApiRequestInfo dataApiRequestInfo = new DataApiRequestInfo(Optional.of(sessionId));
    OperationsConfig operationsConfig = OfflineFileWriterInitializer.buildOperationsConfig();
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(operationsConfig, new SimpleMeterRegistry());
    QueryExecutor queryExecutor = new QueryExecutor(cqlSessionCache, operationsConfig);
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
