package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
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
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.Date;
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
  private static final CommandContext COMMAND_CONTEXT =
      new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  @Inject QueryExecutor queryExecutor;
  @Inject ObjectMapper objectMapper;

  @Nested
  class Execute {

    @Test
    public void findAll() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" LIMIT %s"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME, 20);

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
              .withPageSize(20)
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

      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(),
              DocumentProjector.identityProjector(),
              null,
              20,
              20,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents())
          .hasSize(2)
          .containsOnly(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void byIdWithInOperator() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 2"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
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
            "username": "user1"
          }
          """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
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
                          Values.of(doc1))));

      ValidatingStargateBridge.QueryAssert candidatesAssert2 =
          withQuery(
                  collectionReadCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))))
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
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2))));

      DBFilterBase.IDFilter filter =
          new DBFilterBase.IDFilter(
              DBFilterBase.IDFilter.Operator.IN,
              List.of(DocumentId.fromString("doc1"), DocumentId.fromString("doc2")));
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              2,
              2,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();
      candidatesAssert2.assertExecuteCount().isOne();
      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents())
          .hasSize(2)
          .contains(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void byIdWithInEmptyArray() throws Exception {
      DBFilterBase.IDFilter filter =
          new DBFilterBase.IDFilter(DBFilterBase.IDFilter.Operator.IN, List.of());
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              2,
              2,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents()).hasSize(0);
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void byIdWithInAndOtherOperator() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? AND key = ? LIMIT 2"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
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
                "username": "user1"
              }
              """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()),
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
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
                          Values.of(doc1))));

      ValidatingStargateBridge.QueryAssert candidatesAssert2 =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()),
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))))
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
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2))));

      DBFilterBase.IDFilter filter =
          new DBFilterBase.IDFilter(
              DBFilterBase.IDFilter.Operator.IN,
              List.of(DocumentId.fromString("doc1"), DocumentId.fromString("doc2")));
      DBFilterBase.TextFilter textFilter =
          new DBFilterBase.TextFilter("username", DBFilterBase.TextFilter.Operator.EQ, "user1");
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter, textFilter),
              DocumentProjector.identityProjector(),
              null,
              2,
              2,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();
      candidatesAssert2.assertExecuteCount().isOne();
      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents())
          .hasSize(2)
          .contains(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findOneByIdWithInOperator() throws Exception {
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
      String doc2 =
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
                          Values.of(doc1))));

      ValidatingStargateBridge.QueryAssert candidatesAssert2 =
          withQuery(
                  collectionReadCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))))
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
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2))));

      DBFilterBase.IDFilter filter =
          new DBFilterBase.IDFilter(
              DBFilterBase.IDFilter.Operator.IN,
              List.of(DocumentId.fromString("doc1"), DocumentId.fromString("doc2")));
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              2,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();
      candidatesAssert2.assertExecuteCount().isOne();
      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents())
          .hasSize(1)
          .containsAnyOf(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
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

      DBFilterBase.IDFilter filter =
          new DBFilterBase.IDFilter(
              DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"));
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents()).hasSize(1).containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithIdNoData() {
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

      DBFilterBase.IDFilter filter =
          new DBFilterBase.IDFilter(
              DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"));
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents()).isEmpty();
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
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

      DBFilterBase.TextFilter filter =
          new DBFilterBase.TextFilter("username", DBFilterBase.MapFilterBase.Operator.EQ, "user1");
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents()).hasSize(1).containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
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

      DBFilterBase.BoolFilter filter =
          new DBFilterBase.BoolFilter(
              "registration_active", DBFilterBase.MapFilterBase.Operator.EQ, true);
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents()).hasSize(1).containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithDateFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                  {
                    "_id": "doc1",
                    "username": "user1",
                    "registration_active" : true,
                    "date_field" : {"$date" : 1672531200000}
                  }
                  """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of(
                      "date_field "
                          + new DocValueHasher().getHash(new Date(1672531200000L)).hash()))
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

      DBFilterBase.DateFilter filter =
          new DBFilterBase.DateFilter(
              "date_field", DBFilterBase.MapFilterBase.Operator.EQ, new Date(1672531200000L));
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents()).hasSize(1).containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
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

      DBFilterBase.ExistsFilter filter = new DBFilterBase.ExistsFilter("registration_active", true);
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents()).hasSize(1).containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
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

      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.AllFilter(new DocValueHasher(), "tags", "tag1"),
              new DBFilterBase.AllFilter(new DocValueHasher(), "tags", "tag2"));
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              filters,
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents()).hasSize(1).containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
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

      DBFilterBase.SizeFilter filter = new DBFilterBase.SizeFilter("tags", 2);
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents()).hasSize(1).containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
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

      DBFilterBase.ArrayEqualsFilter filter =
          new DBFilterBase.ArrayEqualsFilter(new DocValueHasher(), "tags", List.of("tag1", "tag2"));
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents()).hasSize(1).containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
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

      DBFilterBase.SubDocEqualsFilter filter =
          new DBFilterBase.SubDocEqualsFilter(
              new DocValueHasher(), "sub_doc", Map.of("col", "val"));
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents()).hasSize(1).containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    /////////////////////
    ///    FAILURES   ///
    /////////////////////

    @Test
    public void failurePropagated() {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      RuntimeException exception = new RuntimeException("Ivan breaks tests.");
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
              .returningFailure(exception);

      DBFilterBase.IDFilter filter =
          new DBFilterBase.IDFilter(
              DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"));
      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      Throwable failure =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      assertThat(failure).isEqualTo(exception);
    }

    @Test
    public void findAllSort() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" LIMIT %s"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME, 20);

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
      String doc3 =
          """
            {
              "_id": "doc3",
              "username": "user3"
            }
            """;
      String doc4 =
          """
            {
              "_id": "doc4",
              "username": "user4"
            }
            """;
      String doc5 =
          """
            {
              "_id": "doc5",
              "username": "user5"
            }
            """;
      String doc6 =
          """
            {
              "_id": "doc6",
              "username": "user6"
            }
            """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql)
              .withPageSize(20)
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_text_values['username']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_dbl_values['username']")
                          .setType(TypeSpecs.DECIMAL)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_bool_values['username']")
                          .setType(TypeSpecs.BOOLEAN)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_null_values['username']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_timestamp_values['username']")
                          .setType(TypeSpecs.DATE)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc6"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc6),
                          Values.of("user6"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc4"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc4),
                          Values.of("user4"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2),
                          Values.of("user2"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1),
                          Values.of("user1"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc3"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc3),
                          Values.of("user3"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc5"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc5),
                          Values.of("user5"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL)));

      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(),
              DocumentProjector.identityProjector(),
              null,
              5,
              20,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", true)),
              0,
              20);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents())
          .hasSize(5)
          .isEqualTo(
              List.of(
                  objectMapper.readTree(doc1),
                  objectMapper.readTree(doc2),
                  objectMapper.readTree(doc3),
                  objectMapper.readTree(doc4),
                  objectMapper.readTree(doc5)));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findAllSortByDate() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['sort_date'], query_dbl_values['sort_date'], query_bool_values['sort_date'], query_null_values['sort_date'], query_timestamp_values['sort_date'] FROM \"%s\".\"%s\" LIMIT %s"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME, 20);

      String doc1 =
          """
                {
                  "_id": "doc1",
                  "username": "user1",
                  "sort_date": {
                    "$date": 1672531200000
                  }
                }
                """;
      String doc2 =
          """
                {
                  "_id": "doc2",
                  "username": "user2",
                  "sort_date": {
                    "$date": 1672531300000
                  }
                }
                """;
      String doc3 =
          """
                {
                  "_id": "doc3",
                  "username": "user3",
                  "sort_date": {
                    "$date": 1672531400000
                  }
                }
                """;
      String doc4 =
          """
                {
                  "_id": "doc4",
                  "username": "user4"
                  ,
                  "sort_date": {
                    "$date": 1672531500000
                  }
                }
                """;
      String doc5 =
          """
                {
                  "_id": "doc5",
                  "username": "user5",
                  "sort_date": {
                    "$date": 1672531600000
                  }
                }
                """;
      String doc6 =
          """
                {
                  "_id": "doc6",
                  "username": "user6",
                  "sort_date": {
                    "$date": 1672531700000
                  }
                }
                """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql)
              .withPageSize(20)
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_text_values['sort_date']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_dbl_values['sort_date']")
                          .setType(TypeSpecs.DECIMAL)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_bool_values['sort_date']")
                          .setType(TypeSpecs.BOOLEAN)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_null_values['sort_date']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_timestamp_values['sort_date']")
                          .setType(TypeSpecs.DATE)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc6"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc6),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.of(1672531700000L)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc4"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc4),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.of(1672531500000L)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.of(1672531300000L)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.of(1672531200000L)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc3"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc3),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.of(1672531400000L)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc5"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc5),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.of(1672531600000L))));

      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(),
              DocumentProjector.identityProjector(),
              null,
              5,
              20,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("sort_date", true)),
              0,
              20);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents())
          .hasSize(5)
          .isEqualTo(
              List.of(
                  objectMapper.readTree(doc1),
                  objectMapper.readTree(doc2),
                  objectMapper.readTree(doc3),
                  objectMapper.readTree(doc4),
                  objectMapper.readTree(doc5)));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findAllSortWithSkip() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" LIMIT %s"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME, 20);

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
      String doc3 =
          """
            {
              "_id": "doc3",
              "username": "user3"
            }
            """;
      String doc4 =
          """
            {
              "_id": "doc4",
              "username": "user4"
            }
            """;
      String doc5 =
          """
            {
              "_id": "doc5",
              "username": "user5"
            }
            """;
      String doc6 =
          """
            {
              "_id": "doc6",
              "username": "user6"
            }
            """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql)
              .withPageSize(20)
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_text_values['username']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_dbl_values['username']")
                          .setType(TypeSpecs.DECIMAL)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_bool_values['username']")
                          .setType(TypeSpecs.BOOLEAN)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_null_values['username']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_timestamp_values['username']")
                          .setType(TypeSpecs.DATE)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc6"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc6),
                          Values.of("user6"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc4"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc4),
                          Values.of("user4"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2),
                          Values.of("user2"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1),
                          Values.of("user1"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc3"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc3),
                          Values.of("user3"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc5"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc5),
                          Values.of("user5"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL)));

      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(),
              DocumentProjector.identityProjector(),
              null,
              5,
              20,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", true)),
              5,
              20);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents())
          .hasSize(1)
          .isEqualTo(List.of(objectMapper.readTree(doc6)));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findAllSortDescending() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" LIMIT %s"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME, 20);

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
      String doc3 =
          """
            {
              "_id": "doc3",
              "username": "user3"
            }
            """;
      String doc4 =
          """
            {
              "_id": "doc4",
              "username": "user4"
            }
            """;
      String doc5 =
          """
            {
              "_id": "doc5",
              "username": "user5"
            }
            """;
      String doc6 =
          """
            {
              "_id": "doc6",
              "username": "user6"
            }
            """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql)
              .withPageSize(20)
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
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_text_values['username']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_dbl_values['username']")
                          .setType(TypeSpecs.DECIMAL)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_bool_values['username']")
                          .setType(TypeSpecs.BOOLEAN)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_null_values['username']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_timestamp_values['username']")
                          .setType(TypeSpecs.DATE)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc6"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc6),
                          Values.of("user6"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc4"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc4),
                          Values.of("user4"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2),
                          Values.of("user2"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1),
                          Values.of("user1"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc3"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc3),
                          Values.of("user3"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc5"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc5),
                          Values.of("user5"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL)));

      FindOperation operation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(),
              DocumentProjector.identityProjector(),
              null,
              5,
              20,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", false)),
              0,
              20);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().documents())
          .hasSize(5)
          .isEqualTo(
              List.of(
                  objectMapper.readTree(doc6),
                  objectMapper.readTree(doc5),
                  objectMapper.readTree(doc4),
                  objectMapper.readTree(doc3),
                  objectMapper.readTree(doc2)));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }
  }

  @Nested
  class GetDocuments {

    @Test
    public void findWithId() {
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

      DBFilterBase.IDFilter filter =
          new DBFilterBase.IDFilter(
              DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"));
      FindOperation findOperation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      ReadOperation.FindResponse result =
          findOperation
              .getDocuments(queryExecutor, null, null)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      assertThat(result.docs()).isNotNull();
      assertThat(result.docs()).hasSize(1);
    }

    @Test
    public void findWithIdWithIdRetry() {
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

      DBFilterBase.IDFilter filter =
          new DBFilterBase.IDFilter(
              DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"));
      FindOperation findOperation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      ReadOperation.FindResponse result =
          findOperation
              .getDocuments(queryExecutor, null, filter)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      assertThat(result.docs()).isNotNull();
      assertThat(result.docs()).hasSize(1);
    }

    @Test
    public void findWithDynamic() {
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

      DBFilterBase.TextFilter filter =
          new DBFilterBase.TextFilter("username", DBFilterBase.MapFilterBase.Operator.EQ, "user1");
      FindOperation findOperation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      ReadOperation.FindResponse result =
          findOperation
              .getDocuments(queryExecutor, null, null)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      assertThat(result.docs()).isNotNull();
      assertThat(result.docs()).hasSize(1);
    }

    @Test
    public void findWithDynamicWithIdRetry() {
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

      DBFilterBase.TextFilter filter =
          new DBFilterBase.TextFilter("username", DBFilterBase.MapFilterBase.Operator.EQ, "user1");
      FindOperation findOperation =
          new FindOperation(
              COMMAND_CONTEXT,
              List.of(filter),
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper,
              null,
              0,
              0);

      DBFilterBase.IDFilter idFilter =
          new DBFilterBase.IDFilter(
              DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"));
      ReadOperation.FindResponse result =
          findOperation
              .getDocuments(queryExecutor, null, idFilter)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      assertThat(result.docs()).isNotNull();
      assertThat(result.docs()).hasSize(1);
    }
  }
}
