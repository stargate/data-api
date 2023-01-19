package io.stargate.sgv3.docsapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import io.stargate.sgv3.docsapi.api.model.command.CommandStatus;
import io.stargate.sgv3.docsapi.service.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv3.docsapi.service.bridge.ValidatingStargateBridge;
import io.stargate.sgv3.docsapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv3.docsapi.service.bridge.executor.QueryExecutor;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DeleteOperationTest extends AbstractValidatingStargateBridgeTest {
  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  private CommandContext commandContext = new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  @Inject QueryExecutor queryExecutor;
  @Inject DocumentConfig documentConfig;
  @Inject ObjectMapper objectMapper;

  @Nested
  class DeleteOperationsTest {

    @Test
    public void deleteWithId() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      UUID tx_id = UUID.randomUUID();
      ValidatingStargateBridge.QueryAssert readAssert =
          withQuery(collectionReadCql, Values.of("doc1"))
              .withPageSize(documentConfig.defaultPageSize())
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(List.of(List.of(Values.of("doc1"), Values.of(tx_id))));

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteAssert =
          withQuery(collectionDeleteCql, Values.of("doc1"), Values.of(tx_id))
              .returning(List.of(List.of(Values.of(true))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(new FindOperation.IDFilter(FindOperation.IDFilter.Operator.EQ, "doc1")),
              null,
              1,
              false,
              objectMapper);

      DeleteOperation operation = new DeleteOperation(commandContext, findOperation);
      final Supplier<CommandResult> execute =
          operation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.DELETED_IDS)).isNotNull();
                assertThat((List<String>) result.status().get(CommandStatus.DELETED_IDS))
                    .hasSize(1);
                assertThat((List<String>) result.status().get(CommandStatus.DELETED_IDS))
                    .contains("doc1");
              });
    }

    @Test
    public void deleteWithIdNoData() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      UUID tx_id = UUID.randomUUID();
      ValidatingStargateBridge.QueryAssert readAssert =
          withQuery(collectionReadCql, Values.of("doc1"))
              .withPageSize(documentConfig.defaultPageSize())
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(List.of());

      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(new FindOperation.IDFilter(FindOperation.IDFilter.Operator.EQ, "doc1")),
              null,
              1,
              false,
              objectMapper);

      DeleteOperation operation = new DeleteOperation(commandContext, findOperation);
      final Supplier<CommandResult> execute =
          operation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.DELETED_IDS)).isNotNull();
                assertThat((List<String>) result.status().get(CommandStatus.DELETED_IDS))
                    .hasSize(0);
              });
    }

    @Test
    public void deleteWithDynamic() throws Exception {
      UUID tx_id = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE query_text_values[?] = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql, Values.of("username"), Values.of("user1"))
              .withPageSize(documentConfig.defaultPageSize())
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(List.of(List.of(Values.of("doc1"), Values.of(tx_id))));
      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteAssert =
          withQuery(collectionDeleteCql, Values.of("doc1"), Values.of(tx_id))
              .returning(List.of(List.of(Values.of(true))));

      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new FindOperation.TextFilter(
                      "username", FindOperation.MapFilterBase.Operator.EQ, "user1")),
              null,
              1,
              false,
              objectMapper);

      DeleteOperation operation = new DeleteOperation(commandContext, findOperation);
      final Supplier<CommandResult> execute =
          operation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.DELETED_IDS)).isNotNull();
                assertThat((List<String>) result.status().get(CommandStatus.DELETED_IDS))
                    .hasSize(1);
                assertThat((List<String>) result.status().get(CommandStatus.DELETED_IDS))
                    .contains("doc1");
              });
    }

    @Test
    public void deleteWithNoResult() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE query_text_values[?] = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                               {
                                     "_id": "doc1",
                                     "username": "user1"
                                   }
                               """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql, Values.of("username"), Values.of("user1"))
              .withPageSize(documentConfig.defaultPageSize())
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(List.of());
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new FindOperation.TextFilter(
                      "username", FindOperation.MapFilterBase.Operator.EQ, "user1")),
              null,
              1,
              false,
              objectMapper);
      DeleteOperation operation = new DeleteOperation(commandContext, findOperation);
      final Supplier<CommandResult> execute =
          operation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.DELETED_IDS)).isNotNull();
                assertThat((List<String>) result.status().get(CommandStatus.DELETED_IDS))
                    .hasSize(0);
              });
    }
  }
}
