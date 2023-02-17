package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.common.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import java.util.List;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class InsertOperationTest extends AbstractValidatingStargateBridgeTest {
  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  private CommandContext commandContext = new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  @Inject Shredder shredder;
  @Inject ObjectMapper objectMapper;
  @Inject QueryExecutor queryExecutor;

  @Nested
  class InsertOperationsTest {

    @Test
    public void insertOne() throws Exception {
      String doc1 =
          """
                    {
                          "_id": "doc1",
                          "text": "user1",
                          "number" : 10,
                          "boolean": true,
                          "nullval" : null,
                          "array" : ["a", "b"],
                          "sub_doc" : {"col": "val"}
                        }
                    """;
      String insert =
          "INSERT INTO %s.%s"
              + "            (key, tx_id, doc_json, doc_properties, exist_keys, sub_doc_equals, array_size, array_equals, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values)"
              + "        VALUES"
              + "            (?, now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  IF NOT EXISTS";
      String collectionInsertCql = insert.formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final JsonNode jsonNode = objectMapper.readTree(doc1);
      final WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionInsertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                  Values.of(shredDocument.docJson()),
                  Values.of(
                      CustomValueSerializers.getIntegerMapValues(shredDocument.docProperties())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument.arraySize())),
                  Values.of(CustomValueSerializers.getStringMapValues(shredDocument.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(shredDocument.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .returning(List.of(List.of(Values.of(true))));

      InsertOperation operation = new InsertOperation(commandContext, shredDocument);
      final Uni<Supplier<CommandResult>> execute = operation.execute(queryExecutor);
      final CommandResult commandResultSupplier =
          execute.subscribe().asCompletionStage().get().get();

      UniAssertSubscriber<Supplier<CommandResult>> subscriber =
          operation.execute(queryExecutor).subscribe().withSubscriber(UniAssertSubscriber.create());

      assertThat(commandResultSupplier)
          .satisfies(
              commandResult -> {
                assertThat(commandResultSupplier.status()).isNotNull();
                assertThat(commandResultSupplier.status().get(CommandStatus.INSERTED_IDS))
                    .isNotNull();
                assertThat(
                        (List<DocumentId>)
                            commandResultSupplier.status().get(CommandStatus.INSERTED_IDS))
                    .contains(new DocumentId.StringId("doc1"));
              });
    }
  }

  @Test
  public void insertDuplicate() throws Exception {
    String doc1 =
        """
                      {
                            "_id": "doc1",
                            "text": "user1",
                            "number" : 10,
                            "boolean": true,
                            "nullval" : null,
                            "array" : ["a", "b"],
                            "sub_doc" : {"col": "val"}
                          }
                      """;
    String insert =
        "INSERT INTO %s.%s"
            + "            (key, tx_id, doc_json, doc_properties, exist_keys, sub_doc_equals, array_size, array_equals, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values)"
            + "        VALUES"
            + "            (?, now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  IF NOT EXISTS";
    String collectionInsertCql = insert.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    final JsonNode jsonNode = objectMapper.readTree(doc1);
    final WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

    ValidatingStargateBridge.QueryAssert candidatesAssert =
        withQuery(
                collectionInsertCql,
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(shredDocument.docJson()),
                Values.of(
                    CustomValueSerializers.getIntegerMapValues(shredDocument.docProperties())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.existKeys())),
                Values.of(CustomValueSerializers.getStringMapValues(shredDocument.subDocEquals())),
                Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument.arraySize())),
                Values.of(CustomValueSerializers.getStringMapValues(shredDocument.arrayEquals())),
                Values.of(CustomValueSerializers.getStringSetValue(shredDocument.arrayContains())),
                Values.of(
                    CustomValueSerializers.getBooleanMapValues(shredDocument.queryBoolValues())),
                Values.of(
                    CustomValueSerializers.getDoubleMapValues(shredDocument.queryNumberValues())),
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .returning(List.of(List.of(Values.of(false))));

    InsertOperation operation = new InsertOperation(commandContext, shredDocument);
    UniAssertSubscriber<Supplier<CommandResult>> subscriber =
        operation.execute(queryExecutor).subscribe().withSubscriber(UniAssertSubscriber.create());
    subscriber.assertFailedWith(
        JsonApiException.class, "Document already exists with the _id: doc1");
  }
}
