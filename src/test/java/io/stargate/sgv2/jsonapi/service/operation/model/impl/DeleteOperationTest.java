package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.common.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
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
          withQuery(
                  collectionReadCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id))));

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteAssert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id))
              .returning(List.of(List.of(Values.of(true))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"))),
              null,
              1,
              1,
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = new DeleteOperation(commandContext, findOperation);
      final Supplier<CommandResult> execute =
          operation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.DELETED_IDS)).isNotNull();
                assertThat((List<DocumentId>) result.status().get(CommandStatus.DELETED_IDS))
                    .hasSize(1);
                assertThat((List<DocumentId>) result.status().get(CommandStatus.DELETED_IDS))
                    .contains(DocumentId.fromString("doc1"));
              });
    }

    @Test
    public void deleteWithIdNoData() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      UUID tx_id = UUID.randomUUID();
      ValidatingStargateBridge.QueryAssert readAssert =
          withQuery(
                  collectionReadCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
              .withPageSize(1)
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
              List.of(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"))),
              null,
              1,
              1,
              ReadType.KEY,
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
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
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
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id))));
      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteAssert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id))
              .returning(List.of(List.of(Values.of(true))));

      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.TextFilter(
                      "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1")),
              null,
              1,
              1,
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = new DeleteOperation(commandContext, findOperation);
      final Supplier<CommandResult> execute =
          operation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.DELETED_IDS)).isNotNull();
                assertThat((List<DocumentId>) result.status().get(CommandStatus.DELETED_IDS))
                    .hasSize(1);
                assertThat((List<DocumentId>) result.status().get(CommandStatus.DELETED_IDS))
                    .contains(DocumentId.fromString("doc1"));
              });
    }

    @Test
    public void deleteManyWithDynamic() throws Exception {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 2"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(2)
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
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id1)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(tx_id2))));
      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      withQuery(
              collectionDeleteCql,
              Values.of(CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
              Values.of(tx_id1))
          .returning(List.of(List.of(Values.of(true))));

      withQuery(
              collectionDeleteCql,
              Values.of(CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))),
              Values.of(tx_id2))
          .returning(List.of(List.of(Values.of(true))));

      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.TextFilter(
                      "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1")),
              null,
              2,
              2,
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = new DeleteOperation(commandContext, findOperation);
      final Supplier<CommandResult> execute =
          operation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.DELETED_IDS)).isNotNull();
                assertThat((List<DocumentId>) result.status().get(CommandStatus.DELETED_IDS))
                    .hasSize(2);
                assertThat((List<DocumentId>) result.status().get(CommandStatus.DELETED_IDS))
                    .contains(DocumentId.fromString("doc1"), DocumentId.fromString("doc2"));
              });
    }

    @Test
    public void deleteWithNoResult() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                               {
                                     "_id": "doc1",
                                     "username": "user1"
                                   }
                               """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
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
                  new DBFilterBase.TextFilter(
                      "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1")),
              null,
              1,
              1,
              ReadType.KEY,
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
