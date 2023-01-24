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
import io.stargate.sgv3.docsapi.service.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv3.docsapi.service.bridge.ValidatingStargateBridge;
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
              .returning(
                  List.of(
                      List.of(Values.of("doc1"), Values.of(UUID.randomUUID()), Values.of(doc1)),
                      List.of(Values.of("doc2"), Values.of(UUID.randomUUID()), Values.of(doc2))));
      FindOperation findOperation =
          new FindOperation(commandContext, List.of(), null, 2, 2, true, objectMapper);
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
          withQuery(collectionReadCql, Values.of("doc1"))
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
              .returning(
                  List.of(
                      List.of(Values.of("doc1"), Values.of(UUID.randomUUID()), Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(new FindOperation.IDFilter(FindOperation.IDFilter.Operator.EQ, "doc1")),
              null,
              1,
              1,
              true,
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
          withQuery(collectionReadCql, Values.of("doc1"))
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
              List.of(new FindOperation.IDFilter(FindOperation.IDFilter.Operator.EQ, "doc1")),
              null,
              1,
              1,
              true,
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
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE query_text_values[?] = ? LIMIT 1"
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
              .returning(
                  List.of(
                      List.of(Values.of("doc1"), Values.of(UUID.randomUUID()), Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new FindOperation.TextFilter(
                      "username", FindOperation.MapFilterBase.Operator.EQ, "user1")),
              null,
              1,
              1,
              true,
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
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE query_bool_values[?] = ? LIMIT 1"
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
          withQuery(collectionReadCql, Values.of("registration_active"), Values.of((byte) 1))
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
              .returning(
                  List.of(
                      List.of(Values.of("doc1"), Values.of(UUID.randomUUID()), Values.of(doc1))));
      FindOperation findOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new FindOperation.BoolFilter(
                      "registration_active", FindOperation.MapFilterBase.Operator.EQ, true)),
              null,
              1,
              1,
              true,
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
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE query_text_values[?] = ? LIMIT 1"
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
                  new FindOperation.TextFilter(
                      "username", FindOperation.MapFilterBase.Operator.EQ, "user1")),
              null,
              1,
              1,
              true,
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
  }
}
