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
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindOperationTest extends AbstractValidatingStargateBridgeTest {

  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  private CommandContext commandContext = new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  @Inject QueryExecutor queryExecutor;
  @Inject ObjectMapper objectMapper;

  @Nested
  class FindOperationsTest {

    @Test
    public void findAll() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" LIMIT %s"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME, 2);
      String doc1 =
          """
              {
                    "_id": "doc1",
                    "username": "user1"
                  }
              """;
      String doc2 =
          """
              {
                    "_id": "doc2",
                    "username": "user2"
                  }
              """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql)
              .withPageSize(2)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2))));
      FindOperation findOperation =
          new FindOperation(commandContext, List.of(), null, 2, 2, ReadType.DOCUMENT, objectMapper);
      final Supplier<CommandResult> execute =
          findOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.data()).isNotNull();
                assertThat(result.data().docs()).hasSize(2);
              });
    }

    @Test
    public void findWithId() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"))),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      final Supplier<CommandResult> execute =
          findOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.data()).isNotNull();
                assertThat(result.data().docs()).hasSize(1);
              });
    }

    @Test
    public void findWithIdNoData() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
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
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"))),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      final Supplier<CommandResult> execute =
          findOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.data()).isNotNull();
                assertThat(result.data().docs()).hasSize(0);
              });
    }

    @Test
    public void findWithDynamic() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
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
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.TextFilter(
                      "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1")),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      final Supplier<CommandResult> execute =
          findOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.data()).isNotNull();
                assertThat(result.data().docs()).hasSize(1);
              });
    }

    @Test
    public void findWithBooleanFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                          {
                                "_id": "doc1",
                                "username": "user1",
                                "registration_active" : true
                              }
                          """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("registration_active " + new DocValueHasher().getHash(true).hash()))
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.BoolFilter(
                      "registration_active", DBFilterBase.MapFilterBase.Operator.EQ, true)),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      final Supplier<CommandResult> execute =
          findOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.data()).isNotNull();
                assertThat(result.data().docs()).hasSize(1);
              });
    }

    @Test
    public void findWithExistsFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE exist_keys CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                                  {
                                        "_id": "doc1",
                                        "username": "user1",
                                        "registration_active" : true
                                      }
                                  """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql, Values.of("registration_active"))
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(new DBFilterBase.ExistsFilter("registration_active", true)),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      final Supplier<CommandResult> execute =
          findOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.data()).isNotNull();
                assertThat(result.data().docs()).hasSize(1);
              });
    }

    @Test
    public void findWithAllFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? AND array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                      {
                            "_id": "doc1",
                            "username": "user1",
                            "registration_active" : true,
                            "tags": ["tag1", "tag2"]
                          }
                      """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql, Values.of("tags Stag1"), Values.of("tags Stag2"))
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.AllFilter(new DocValueHasher(), "tags", "tag1"),
                  new DBFilterBase.AllFilter(new DocValueHasher(), "tags", "tag2")),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      final Supplier<CommandResult> execute =
          findOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.data()).isNotNull();
                assertThat(result.data().docs()).hasSize(1);
              });
    }

    @Test
    public void findWithSizeFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_size[?] = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                                          {
                                                "_id": "doc1",
                                                "username": "user1",
                                                "registration_active" : true,
                                                "tags" : ["tag1","tag2"]
                                              }
                                          """;

      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql, Values.of("tags"), Values.of(2))
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(new DBFilterBase.SizeFilter("tags", 2)),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      final Supplier<CommandResult> execute =
          findOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.data()).isNotNull();
                assertThat(result.data().docs()).hasSize(1);
              });
    }

    @Test
    public void findWithArrayEqualFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_equals[?] = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                      {
                            "_id": "doc1",
                            "username": "user1",
                            "registration_active" : true,
                            "tags" : ["tag1","tag2"]
                          }
                      """;
      final String hash = new DocValueHasher().getHash(List.of("tag1", "tag2")).hash();
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql, Values.of("tags"), Values.of(hash))
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.ArrayEqualsFilter(
                      new DocValueHasher(), "tags", List.of("tag1", "tag2"))),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      final Supplier<CommandResult> execute =
          findOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.data()).isNotNull();
                assertThat(result.data().docs()).hasSize(1);
              });
    }

    @Test
    public void findWithSubDocEqualFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE sub_doc_equals[?] = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                              {
                                    "_id": "doc1",
                                    "username": "user1",
                                    "registration_active" : true,
                                    "sub_doc" : {"col":"val"}
                                  }
                              """;
      final String hash = new DocValueHasher().getHash(Map.of("col", "val")).hash();
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql, Values.of("sub_doc"), Values.of(hash))
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.SubDocEqualsFilter(
                      new DocValueHasher(), "sub_doc", Map.of("col", "val"))),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      final Supplier<CommandResult> execute =
          findOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.data()).isNotNull();
                assertThat(result.data().docs()).hasSize(1);
              });
    }

    @Test
    public void findWithNoResult() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
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
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
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
              ReadType.DOCUMENT,
              objectMapper);
      final Supplier<CommandResult> execute =
          findOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.data()).isNotNull();
                assertThat(result.data().docs()).hasSize(0);
              });
    }

    @Test
    public void findWithIdWithIdRetry() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"))),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      final ReadOperation.FindResponse result =
          findOperation
              .getDocuments(
                  queryExecutor,
                  null,
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")))
              .subscribeAsCompletionStage()
              .get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.docs()).isNotNull();
                assertThat(result.docs()).hasSize(1);
              });
    }

    @Test
    public void findWithDynamicGetDocument() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
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
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.TextFilter(
                      "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1")),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      final ReadOperation.FindResponse result =
          findOperation.getDocuments(queryExecutor, null, null).subscribeAsCompletionStage().get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.docs()).isNotNull();
                assertThat(result.docs()).hasSize(1);
              });
    }

    @Test
    public void findWithDynamicWithIdRetry() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? AND key = ? LIMIT 1"
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
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()),
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.TextFilter(
                      "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1")),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      final ReadOperation.FindResponse result =
          findOperation
              .getDocuments(
                  queryExecutor,
                  null,
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")))
              .subscribeAsCompletionStage()
              .get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.docs()).isNotNull();
                assertThat(result.docs()).hasSize(1);
              });
    }
  }
}
