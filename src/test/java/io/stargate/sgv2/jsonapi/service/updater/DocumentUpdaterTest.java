package io.stargate.sgv2.jsonapi.service.updater;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizerService;
import io.stargate.sgv2.jsonapi.service.embedding.operation.TestEmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DocumentUpdaterTest {
  @Inject ObjectMapper objectMapper;
  @Inject DataVectorizerService dataVectorizerService;
  private TestEmbeddingProvider testEmbeddingProvider = new TestEmbeddingProvider();

  private static String BASE_DOC_JSON =
      """
      {
          "_id": "1",
          "location": "London"
      }
      """;
  private static String BASE_DOC_JSON_VECTOR =
      """
    {
        "_id": "1",
        "location": "London",
        "$vector": [0.11, 0.22, 0.33],
        "$vectorize": "London City"
    }
    """;

  @Nested
  class UpdateDocumentHappy {

    @Test
    public void setUpdateCondition() throws Exception {
      String expected =
          """
            {
                "_id": "1",
                "location": "New York"
            }
          """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON);
      JsonNode expectedData = objectMapper.readTree(expected);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET,
                  objectMapper.getNodeFactory().objectNode().put("location", "New York")));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.modified()).isEqualTo(true);
              });
    }

    @Test
    public void setUpdateNewData() throws Exception {
      String expected =
          """
            {
                "_id": "1",
                "location": "London",
                "new_data" : "data"
            }
          """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON);
      JsonNode expectedData = objectMapper.readTree(expected);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET,
                  objectMapper.getNodeFactory().objectNode().put("new_data", "data")));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.modified()).isEqualTo(true);
              });
    }

    @Test
    public void setUpdateVector() throws Exception {
      String expected =
          """
                    {
                        "_id": "1",
                        "location": "London",
                        "new_data" : "data"
                    }
                  """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON);
      JsonNode expectedData = objectMapper.readTree(expected);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET,
                  objectMapper.getNodeFactory().objectNode().put("new_data", "data")));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.modified()).isEqualTo(true);
              });
    }

    @Test
    public void setVectorData() throws Exception {
      String expected =
          """
            {
                "_id": "1",
                "location": "London",
                "$vector" : [0.25, 0.25, 0.25]
            }
            """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR);
      JsonNode expectedData = objectMapper.readTree(expected);
      String vectorData =
          """
              {"$vector" : [0.25, 0.25, 0.25] }
              """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(vectorData)));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.modified()).isEqualTo(true);
              });
    }

    @Test
    public void unsetVectorData() throws Exception {
      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR);
      JsonNode expectedData = objectMapper.readTree(BASE_DOC_JSON);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.UNSET,
                  objectMapper.getNodeFactory().objectNode().put("$vector", "")));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.modified()).isEqualTo(true);
              });
    }

    @Test
    public void setUpdateNumberData() throws Exception {
      String expected =
          """
            {
                "_id": "1",
                "location": "London",
                "new_data" : 40
            }
          """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON);
      JsonNode expectedData = objectMapper.readTree(expected);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET,
                  objectMapper.getNodeFactory().objectNode().put("new_data", 40)));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.modified()).isEqualTo(true);
              });
    }

    @Test
    public void unsetUpdateData() throws Exception {
      String expected =
          """
            {
                "_id": "1",
                "location": "London"
            }
          """;

      ObjectNode baseData = (ObjectNode) objectMapper.readTree(BASE_DOC_JSON);
      baseData.put("col", "data");
      JsonNode expectedData = objectMapper.readTree(expected);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.UNSET, objectMapper.getNodeFactory().objectNode().put("col", 1)));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.modified()).isEqualTo(true);
              });
    }
  }

  @Nested
  class UpdateDocumentInvalid {
    @Test
    public void invalidUpdateOperator() throws Exception {
      String updateClause =
          """
                   {"location": "New York"},
              """;
      Throwable t =
          catchThrowable(
              () -> {
                DocumentUpdater.construct(objectMapper.readValue(updateClause, UpdateClause.class));
              });
      assertThat(t)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION)
          .hasMessageStartingWith(
              "Unsupported update operation: Invalid update operator 'location' (must start with '$')");
    }

    @Test
    public void unsupportedUpdateOperator() throws Exception {
      String updateClause =
          """
                   {"$pullAll": { "count" : 5}}
              """;
      Throwable t =
          catchThrowable(
              () -> {
                DocumentUpdater.construct(objectMapper.readValue(updateClause, UpdateClause.class));
              });
      assertThat(t)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION)
          .hasMessageStartingWith(
              "Unsupported update operation: Unsupported update operator '$pullAll'");
    }

    /** Test for ensuring it is not legal to "$set" document id (_id) */
    @Test
    public void invalidSetDocId() throws Exception {
      Throwable t =
          catchThrowable(
              () -> {
                DocumentUpdater.construct(
                    DocumentUpdaterUtils.updateClause(
                        UpdateOperator.SET,
                        objectMapper.getNodeFactory().objectNode().put("_id", "xyz")));
              });
      assertThat(t)
          .isInstanceOf(UpdateException.class)
          .hasFieldOrPropertyWithValue(
              "code", UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR_FOR_DOC_ID.name())
          .hasFieldOrPropertyWithValue("title", "Update operators cannot be used on _id field")
          .hasMessageContaining("The command used the update operator: $set");
    }

    @Test
    public void invalidUnsetDocId() throws Exception {
      Throwable t =
          catchThrowable(
              () -> {
                DocumentUpdater.construct(
                    DocumentUpdaterUtils.updateClause(
                        UpdateOperator.UNSET,
                        objectMapper.getNodeFactory().objectNode().put("_id", "xyz")));
              });
      assertThat(t)
          .isInstanceOf(UpdateException.class)
          .hasFieldOrPropertyWithValue(
              "code", UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR_FOR_DOC_ID.name())
          .hasFieldOrPropertyWithValue("title", "Update operators cannot be used on _id field")
          .hasMessageContaining("The command used the update operator: $unset");
    }

    @Test
    public void invalidSetAndUnsetSameField() throws Exception {
      Throwable t =
          catchThrowable(
              () -> {
                DocumentUpdater.construct(
                    DocumentUpdaterUtils.updateClause(
                        UpdateOperator.SET,
                        (ObjectNode) objectMapper.readTree("{\"setField\":3, \"common\":true}"),
                        UpdateOperator.UNSET,
                        (ObjectNode) objectMapper.readTree("{\"unsetField\":1, \"common\":1}")));
              });
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageContaining(
              "update operators '$set' and '$unset' must not refer to same path: 'common'");
    }

    @Test
    public void invalidMulAndIncSameFieldNested() {
      Throwable t =
          catchThrowable(
              () ->
                  DocumentUpdater.construct(
                      DocumentUpdaterUtils.updateClause(
                          UpdateOperator.INC,
                          (ObjectNode) objectMapper.readTree("{\"root.x\":-7, \"root.inc\":-3}"),
                          UpdateOperator.MUL,
                          (ObjectNode) objectMapper.readTree("{\"root.mul\":3, \"root.x\":2}"))));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageContaining(
              "update operators '$inc' and '$mul' must not refer to same path: 'root.x'");
    }

    @Test
    public void invalidSetOnParentPath() {
      Throwable t =
          catchThrowable(
              () ->
                  DocumentUpdater.construct(
                      DocumentUpdaterUtils.updateClause(
                          UpdateOperator.SET,
                          (ObjectNode) objectMapper.readTree("{\"root.1\":-7, \"root\":[ ]}"))));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageContaining(
              "Update operator path conflict due to overlap: 'root' ($set) vs 'root.1' ($set)");
    }

    @Test
    public void invalidSetOnParentPathWithDollar() {
      Throwable t =
          catchThrowable(
              () ->
                  DocumentUpdater.construct(
                      DocumentUpdaterUtils.updateClause(
                          UpdateOperator.SET,
                          (ObjectNode)
                              objectMapper.readTree(
                                  """
          {
            "root" : 7,
            "x" : 3,
            "root$x" : 5,
            "y" : 5,
            "root.a" : 3
          }
          """))));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageContaining(
              "Update operator path conflict due to overlap: 'root' ($set) vs 'root.a' ($set)");
    }
  }

  @Nested
  class ReplaceDocumentHappy {
    @Test
    public void replaceDocument() throws Exception {
      String expected =
          """
            {
                "_id": "1",
                "location": "New York",
                "new_data" : 40
            }
          """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON);
      JsonNode expectedData = objectMapper.readTree(expected);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              (ObjectNode)
                  objectMapper.readTree(
                      """
                                                      {
                                                        "location": "New York",
                                                        "new_data" : 40
                                                      }
                                                  """));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.modified()).isEqualTo(true);
              });
    }

    @Test
    public void replaceDocumentSameId() throws Exception {
      String expected =
          """
                            {
                                "_id": "1",
                                "location": "New York",
                                "new_data" : 40
                            }
                          """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON);
      JsonNode expectedData = objectMapper.readTree(expected);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              (ObjectNode)
                  objectMapper.readTree(
                      """
                                                                          {
                                                                            "_id": "1",
                                                                            "location": "New York",
                                                                            "new_data" : 40
                                                                          }
                                                                      """));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.modified()).isEqualTo(true);
              });
    }

    @Test
    public void replaceDifferentId() throws Exception {
      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              (ObjectNode)
                  objectMapper.readTree(
                      """
                                                                        {
                                                                          "_id": "2",
                                                                          "location": "New York",
                                                                          "new_data" : 40
                                                                       }
                                                                    """));
      Throwable t =
          catchThrowable(
              () -> {
                DocumentUpdater.DocumentUpdaterResponse updatedDocument =
                    documentUpdater.apply(baseData, false);
              });
      assertThat(t)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .withFailMessage(ErrorCodeV1.DOCUMENT_REPLACE_DIFFERENT_DOCID.getMessage())
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.DOCUMENT_REPLACE_DIFFERENT_DOCID);
    }

    @Test
    public void replaceEmpty() throws Exception {
      String expected =
          """
                            {
                                "_id": "1"
                            }
                          """;
      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON);
      JsonNode expectedData = objectMapper.readTree(expected);

      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              (ObjectNode)
                  objectMapper.readTree(
                      """
                                                                          {}
                                                                      """));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.modified()).isEqualTo(true);
              });
    }
  }

  @Nested
  class VectorizeUpdate {

    @Test
    public void updateVectorize() throws Exception {
      String updateVectorizeData =
          """
                          {"$vectorize" : "Beijing is a big city", "location" : "Beijing City"}
                          """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(updateVectorizeData)));

      String expected1 =
          """
                            {
                                "_id": "1",
                                "location": "Beijing City",
                                "$vectorize": "Beijing is a big city"
                            }
                          """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON); // location as London
      JsonNode expectedData1 = objectMapper.readTree(expected1);
      DocumentUpdater.DocumentUpdaterResponse firstResponse =
          documentUpdater.apply(baseData, false);
      assertThat(firstResponse)
          .isNotNull()
          .satisfies(
              firstResponseNode -> {
                assertThat(firstResponseNode.document()).isEqualTo(expectedData1);
                assertThat(firstResponseNode.modified()).isEqualTo(true); // modified location
                assertThat(firstResponseNode.embeddingUpdateOperations()).isNotEmpty();
                ;
                assertThat(firstResponseNode.embeddingUpdateOperations().get(0).vectorizeContent())
                    .isEqualTo("Beijing is a big city");
              });

      // Second update will vectorize
      final DocumentUpdater.DocumentUpdaterResponse secondResponse =
          firstResponse
              .updateEmbeddingVector(
                  firstResponse,
                  dataVectorizerService,
                  testEmbeddingProvider.commandContextWithVectorize())
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      String expected2 =
          """
                          {
                              "_id":"1",
                              "location": "Beijing City",
                              "$vectorize" : "Beijing is a big city",
                              "$vector": [0.25,0.25,0.25]
                          }
                          """;
      JsonNode expectedData2 = objectMapper.readTree(expected2);
      assertThat(secondResponse)
          .isNotNull()
          .satisfies(
              secondResponseNode -> {
                assertThat(secondResponseNode.document())
                    .usingRecursiveComparison()
                    .ignoringFields("order")
                    .isEqualTo(expectedData2);
                assertThat(secondResponseNode.modified()).isEqualTo(true); // modified $vector
              });
    }

    @Test
    public void update_noVectorize() throws Exception {
      String updateVectorizeData =
          """
                          {"location" : "Beijing City"}
                          """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(updateVectorizeData)));

      String expected1 =
          """
                            {
                                "_id": "1",
                                "location": "Beijing City"
                            }
                          """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON); // location as London
      JsonNode expectedData1 = objectMapper.readTree(expected1);
      DocumentUpdater.DocumentUpdaterResponse firstResponse =
          documentUpdater.apply(baseData, false);
      assertThat(firstResponse)
          .isNotNull()
          .satisfies(
              firstResponseNode -> {
                assertThat(firstResponseNode.document()).isEqualTo(expectedData1);
                assertThat(firstResponseNode.modified()).isEqualTo(true); // modified location
                assertThat(firstResponseNode.embeddingUpdateOperations())
                    .isEmpty(); // should be null
              });
    }

    @Test
    public void update_notModified() throws Exception {
      String updateVectorizeData =
          """
                          {"location" : "London"}
                          """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(updateVectorizeData)));

      String expected1 =
          """
                            {
                                "_id": "1",
                                "location": "London"
                            }
                          """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON); // location as London
      JsonNode expectedData1 = objectMapper.readTree(expected1);
      DocumentUpdater.DocumentUpdaterResponse firstResponse =
          documentUpdater.apply(baseData, false);
      assertThat(firstResponse)
          .isNotNull()
          .satisfies(
              firstResponseNode -> {
                assertThat(firstResponseNode.document()).isEqualTo(expectedData1);
                assertThat(firstResponseNode.modified()).isEqualTo(false); // not modified
                assertThat(firstResponseNode.embeddingUpdateOperations())
                    .isEmpty(); // should be null
              });
    }

    @Test
    public void update_notModifiedVectorize() throws Exception {
      String updateVectorizeData =
          """
                          {"location" : "London", "$vectorize": "London City"}
                          """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(updateVectorizeData)));

      String expected1 =
          """
                          {
                                "_id": "1",
                                "location": "London",
                                "$vector": [0.11, 0.22, 0.33],
                                "$vectorize": "London City"
                            }
                                """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR); // location as London
      JsonNode expectedData1 = objectMapper.readTree(expected1);
      DocumentUpdater.DocumentUpdaterResponse firstResponse =
          documentUpdater.apply(baseData, false);
      assertThat(firstResponse)
          .isNotNull()
          .satisfies(
              firstResponseNode -> {
                assertThat(firstResponseNode.document()).isEqualTo(expectedData1);
                assertThat(firstResponseNode.modified())
                    .isEqualTo(false); // $vectorize has no diff, not modified
                assertThat(firstResponseNode.embeddingUpdateOperations())
                    .isEmpty(); // should be null
              });
    }

    @Test
    public void update_modifiedVector() throws Exception {
      String updateVectorizeData =
          """
                          {"location" : "London", "$vector": [0.1,0.5,0.3]}
                          """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(updateVectorizeData)));

      String expected1 =
          """
                          {
                                "_id": "1",
                                "location": "London",
                                "$vector": [0.1,0.5,0.3]
                            }
                                """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR); // location as London
      JsonNode expectedData1 = objectMapper.readTree(expected1);
      DocumentUpdater.DocumentUpdaterResponse firstResponse =
          documentUpdater.apply(baseData, false);
      assertThat(firstResponse)
          .isNotNull()
          .satisfies(
              firstResponseNode -> {
                assertThat(firstResponseNode.document()).isEqualTo(expectedData1);
                assertThat(firstResponseNode.modified())
                    .isEqualTo(true); // $vector is updated, $vectorize is not
                assertThat(firstResponseNode.embeddingUpdateOperations())
                    .isEmpty(); // should be null
              });
    }

    @Test
    public void update_vectorizeOverwriteVector() throws Exception {
      String updateVectorizeData =
          """
                          {"location" : "London", "$vector": [0.1,0.9,0.6], "$vectorize":"London is rainy"}
                          """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(updateVectorizeData)));

      String expected1 =
          """
                          {
                                "_id": "1",
                                "location": "London",
                                "$vector": [0.1,0.9,0.6],
                                "$vectorize": "London is rainy"
                            }
                                """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR); // location as London
      JsonNode expectedData1 = objectMapper.readTree(expected1);
      DocumentUpdater.DocumentUpdaterResponse firstResponse =
          documentUpdater.apply(baseData, false);
      assertThat(firstResponse)
          .isNotNull()
          .satisfies(
              firstResponseNode -> {
                assertThat(firstResponseNode.document()).isEqualTo(expectedData1);
                assertThat(firstResponseNode.modified())
                    .isEqualTo(true); // $vector is updated but not overwrite, $vectorize is updated
                assertThat(firstResponseNode.embeddingUpdateOperations()).isNotEmpty();
                ; // not null
              });
      // Second update will vectorize and overwrite $vector
      final DocumentUpdater.DocumentUpdaterResponse secondResponse =
          firstResponse
              .updateEmbeddingVector(
                  firstResponse,
                  dataVectorizerService,
                  testEmbeddingProvider.commandContextWithVectorize())
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
      String expected2 =
          """
                          {
                                "_id": "1",
                                "location": "London",
                                "$vector": [0.25,0.25,0.25],
                                "$vectorize": "London is rainy1"
                          }
                          """;
      JsonNode expectedData2 = objectMapper.readTree(expected2);
      assertThat(secondResponse)
          .isNotNull()
          .satisfies(
              secondResponseNode -> {
                assertThat(secondResponseNode.document())
                    .usingRecursiveComparison()
                    .ignoringFields("order")
                    .isEqualTo(expectedData2);
                assertThat(secondResponseNode.modified()).isEqualTo(true);
              });
    }

    @Test
    public void update_vectorizeBlank() throws JsonProcessingException {

      String updateVectorizeData =
          """
                          {"location" : "London", "$vectorize":""}
                          """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(updateVectorizeData)));

      String expected1 =
          """
                          {
                                "_id": "1",
                                "location": "London",
                                "$vector": null,
                                "$vectorize": ""
                            }
                                """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR); // location as London
      JsonNode expectedData1 = objectMapper.readTree(expected1);
      DocumentUpdater.DocumentUpdaterResponse firstResponse =
          documentUpdater.apply(baseData, false);
      assertThat(firstResponse)
          .isNotNull()
          .satisfies(
              firstResponseNode -> {
                assertThat(firstResponseNode.document()).isEqualTo(expectedData1);
                assertThat(firstResponseNode.modified())
                    .isEqualTo(true); // $vector is updated , $vectorize is updated
                assertThat(firstResponseNode.embeddingUpdateOperations()).isEmpty();
              });
    }

    @Test
    public void update_vectorizeNullValue() throws JsonProcessingException {

      String updateVectorizeData =
          """
                          {"location" : "London", "$vectorize":null}
                          """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(updateVectorizeData)));

      String expected1 =
          """
                          {
                                "_id": "1",
                                "location": "London",
                                "$vector": null,
                                "$vectorize": null
                            }
                                """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR); // location as London
      JsonNode expectedData1 = objectMapper.readTree(expected1);
      DocumentUpdater.DocumentUpdaterResponse firstResponse =
          documentUpdater.apply(baseData, false);
      assertThat(firstResponse)
          .isNotNull()
          .satisfies(
              firstResponseNode -> {
                assertThat(firstResponseNode.document()).isEqualTo(expectedData1);
                assertThat(firstResponseNode.modified())
                    .isEqualTo(true); // $vector is updated , $vectorize is updated
                assertThat(firstResponseNode.embeddingUpdateOperations()).isEmpty();
              });
    }

    @Test
    public void update_vectorizeNonTextualFailure() throws JsonProcessingException {

      String updateVectorizeData =
          """
                          {"location" : "London", "$vectorize":123}
                          """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(updateVectorizeData)));
      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR); // location as London

      Throwable failure =
          catchThrowable(
              () -> {
                DocumentUpdater.DocumentUpdaterResponse firstResponse =
                    documentUpdater.apply(baseData, false);
              });
      assertThat(failure)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.INVALID_VECTORIZE_VALUE_TYPE)
          .hasFieldOrPropertyWithValue("message", "$vectorize value needs to be text value");
    }
  }

  @Nested
  class replaceVectorize {

    @Test
    public void replaceDocument() throws Exception {
      String expected1 =
          """
                      {
                          "_id": "1",
                          "$vectorize" : "random text"
                      }
                    """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON);
      JsonNode expectedData = objectMapper.readTree(expected1);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              (ObjectNode)
                  objectMapper.readTree(
                      """
                                                {
                          "$vectorize" : "random text"
                                                      }
                          """));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.embeddingUpdateOperations()).isNotEmpty();

                assertThat(node.modified()).isEqualTo(true);
              });

      // Second update will vectorize
      final DocumentUpdater.DocumentUpdaterResponse secondResponse =
          updatedDocument
              .updateEmbeddingVector(
                  updatedDocument,
                  dataVectorizerService,
                  testEmbeddingProvider.commandContextWithVectorize())
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      String expected2 =
          """
                                          {
                                                   "_id": "1",
                                "$vectorize" : "random text",
                                "$vector": [0.25,0.25,0.25]
                                          }
                                """;
      JsonNode expectedData2 = objectMapper.readTree(expected2);
      assertThat(secondResponse)
          .isNotNull()
          .satisfies(
              secondResponseNode -> {
                assertThat(secondResponseNode.document())
                    .usingRecursiveComparison()
                    .ignoringFields("order")
                    .isEqualTo(expectedData2);
                assertThat(secondResponseNode.modified()).isEqualTo(true);
                // modified $vector
              });
    }

    @Test
    public void replaceDocument_only_replace_vector() throws Exception {
      String expected1 =
          """
                            {
                                "_id": "1",
                                "$vector": [0.2,0.5,0.7]
                            }
                            """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR);
      JsonNode expectedData = objectMapper.readTree(expected1);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              (ObjectNode)
                  objectMapper.readTree(
                      """
                                                                    {
                                "$vector": [0.2,0.5,0.7]
                                                                          }
                                                                                            """));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.embeddingUpdateOperations()).isEmpty();
                assertThat(node.modified()).isEqualTo(true);
              });
    }

    @Test
    public void replaceDocument_vectorizeBlankTest() throws Exception {
      String expected1 =
          """
                            {
                                "_id": "1",
                                "$vectorize": "",
                                "$vector":null
                            }
                            """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR);
      JsonNode expectedData = objectMapper.readTree(expected1);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              (ObjectNode)
                  objectMapper.readTree(
                      """
                                            {
                                             "$vectorize": ""
                                              }
                                            """));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.embeddingUpdateOperations()).isEmpty();
                assertThat(node.modified()).isEqualTo(true);
              });
    }

    @Test
    public void replaceDocument_vectorizeNonTextFailure() throws Exception {
      String expected1 =
          """
                            {
                                "_id": "1",
                                "$vectorize": "",
                                "$vector":null
                            }
                            """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR);
      JsonNode expectedData = objectMapper.readTree(expected1);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              (ObjectNode)
                  objectMapper.readTree(
                      """
                                            {
                                             "$vectorize": 123
                                              }
                                                   """));
      Throwable failure =
          catchThrowable(
              () -> {
                DocumentUpdater.DocumentUpdaterResponse firstResponse =
                    documentUpdater.apply(baseData, false);
              });
      assertThat(failure)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.INVALID_VECTORIZE_VALUE_TYPE)
          .hasFieldOrPropertyWithValue("message", "$vectorize value needs to be text value");
    }

    @Test
    public void replaceDocument_vectorizeNullValue() throws Exception {
      String expected1 =
          """
                            {
                                "_id": "1",
                                "$vectorize": null,
                                "$vector":null
                            }
                            """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR);
      JsonNode expectedData = objectMapper.readTree(expected1);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              (ObjectNode)
                  objectMapper.readTree(
                      """
                                            {
                                             "$vectorize": null
                                              }
                                                   """));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.embeddingUpdateOperations()).isEmpty();
                assertThat(node.modified()).isEqualTo(true);
              });
    }

    @Test
    public void replaceDocument_allNull() throws Exception {
      String expected1 =
          """
                            {
                                "_id": "123",
                                "$vectorize": null,
                                "$vector":null
                            }
                            """;

      String allNull =
          """
                      {
                          "_id": "123",
                          "$vectorize": null,
                          "$vector":null
                      }
          """;

      JsonNode baseData = objectMapper.readTree(allNull);
      JsonNode expectedData = objectMapper.readTree(expected1);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              (ObjectNode)
                  objectMapper.readTree(
                      """
                                            {
                                             "$vectorize": null
                                              }
                                                   """));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.embeddingUpdateOperations()).isEmpty();
                assertThat(node.modified()).isEqualTo(false); // identical, so not modified
              });
    }

    @Test
    public void replaceDocument_willVectorizeEvenVectorizeHasNoDiff() throws Exception {
      String expected1 =
          """
                                    {
                                        "_id": "1",
                                        "location": "London",
                                        "$vectorize": "London City"
                                    }
                                    """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR);
      JsonNode expectedData = objectMapper.readTree(expected1);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              (ObjectNode)
                  objectMapper.readTree(
                      """
                                          {
                                          "$vectorize": "London City",
                                          "location": "London"
                                                                  }
                                                                       """));
      DocumentUpdater.DocumentUpdaterResponse updatedDocument =
          documentUpdater.apply(baseData, false);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node.document()).isEqualTo(expectedData);
                assertThat(node.embeddingUpdateOperations()).isNotEmpty(); // need to re-vectorize
                assertThat(node.modified())
                    .isEqualTo(
                        true); // not identical, because there is no $vector in replaceDocument
              });

      // Second update will vectorize
      final DocumentUpdater.DocumentUpdaterResponse secondResponse =
          updatedDocument
              .updateEmbeddingVector(
                  updatedDocument,
                  dataVectorizerService,
                  testEmbeddingProvider.commandContextWithVectorize())
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      String expected2 =
          """
                                                  {
                                       "_id": "1",
                                        "location": "London",
                                        "$vectorize": "London City",
                                        "$vector": [0.25,0.25,0.25]
                                                  }
                                                  """;
      JsonNode expectedData2 = objectMapper.readTree(expected2);
      assertThat(secondResponse)
          .isNotNull()
          .satisfies(
              secondResponseNode -> {
                assertThat(secondResponseNode.document())
                    .usingRecursiveComparison()
                    .ignoringFields("order")
                    .isEqualTo(expectedData2);
                assertThat(secondResponseNode.modified()).isEqualTo(true);
                // modified $vector
              });
    }
  }
}
