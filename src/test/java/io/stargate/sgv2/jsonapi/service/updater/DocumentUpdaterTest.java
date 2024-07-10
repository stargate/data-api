package io.stargate.sgv2.jsonapi.service.updater;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizer;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.TestEmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DocumentUpdaterTest {
  @Inject ObjectMapper objectMapper;

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
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION)
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
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION)
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
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .withFailMessage("Should throw exception on $set of _id")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID)
          .hasMessage(ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": $set");
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
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .withFailMessage("Should throw exception on $unset of _id")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID)
          .hasMessage(ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": $unset");
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
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessage("Update operators '$set' and '$unset' must not refer to same path: 'common'");
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
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessage("Update operators '$inc' and '$mul' must not refer to same path: 'root.x'");
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
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessage(
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
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessage(
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
          .withFailMessage(ErrorCode.DOCUMENT_REPLACE_DIFFERENT_DOCID.getMessage())
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCUMENT_REPLACE_DIFFERENT_DOCID);
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
  class VectorizeUpdateTest {

    private final EmbeddingProvider testService = new TestEmbeddingProvider();
    private final CollectionSettings collectionSettings =
        TestEmbeddingProvider.commandContextWithVectorize.collectionSettings();

    @Test
    public void two_levels_update() throws Exception {
      // First level update will skip $vectorize for setOperation
      // vectorization will be done in second level
      String updateVectorizeData =
          """
                                {"$vectorize" : "Beijing is a big city", "location" : "Beijing City"}
                                """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(updateVectorizeData)));

      String expected_level_1 =
          """
                          {
                              "_id": "1",
                              "location": "Beijing City"
                          }
                        """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON); // location as London
      JsonNode expectedData1 = objectMapper.readTree(expected_level_1);
      DocumentUpdater.DocumentUpdaterResponse firstResponse =
          documentUpdater.apply(baseData, false);
      assertThat(firstResponse)
          .isNotNull()
          .satisfies(
              firstResponseNode -> {
                assertThat(firstResponseNode.document()).isEqualTo(expectedData1);
                assertThat(firstResponseNode.modified()).isEqualTo(true); // modified location
              });

      // Second level update will vectorize in setOperation
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      final DocumentUpdater.DocumentUpdaterResponse secondResponse =
          documentUpdater
              .applyUpdateVectorize(firstResponse.document(), false, dataVectorizer)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      String expected_level_2 =
          """
                          {
                              "_id":"1",
                              "location": "Beijing City",
                              "$vectorize" : "Beijing is a big city",
                              "$vector": [0.25,0.25,0.25]
                          }
                          """;
      JsonNode expectedData2 = objectMapper.readTree(expected_level_2);
      assertThat(secondResponse)
          .isNotNull()
          .satisfies(
              secondResponseNode -> {
                assertThat(secondResponseNode.document())
                    .usingRecursiveComparison()
                    .ignoringFields("order")
                    .isEqualTo(expectedData2);
                assertThat(secondResponseNode.modified())
                    .isEqualTo(true); // modified $vectorize and $vector
              });
    }

    @Test
    public void not_modified_for_first_update() throws Exception {
      // First level update will skip $vectorize for setOperation
      // vectorization will be done in second level
      String updateVectorizeData =
          """
                                  {"$vectorize" : "Beijing is a big city"}
                                  """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(updateVectorizeData)));

      String expected_level_1 =
          """
                      {
                          "_id": "1",
                          "location": "London"
                      }
                    """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON); // location as London
      JsonNode expectedData1 = objectMapper.readTree(expected_level_1);
      DocumentUpdater.DocumentUpdaterResponse firstResponse =
          documentUpdater.apply(baseData, false);
      assertThat(firstResponse)
          .isNotNull()
          .satisfies(
              firstResponseNode -> {
                assertThat(firstResponseNode.document()).isEqualTo(expectedData1);
                assertThat(firstResponseNode.modified())
                    .isEqualTo(false); // location is not modified
              });

      // Second level update will vectorize in setOperation
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      final DocumentUpdater.DocumentUpdaterResponse secondResponse =
          documentUpdater
              .applyUpdateVectorize(firstResponse.document(), false, dataVectorizer)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      String expected_level_2 =
          """
                      {
                          "_id":"1",
                          "location": "London",
                          "$vectorize" : "Beijing is a big city",
                          "$vector": [0.25,0.25,0.25]
                      }
                      """;
      JsonNode expectedData2 = objectMapper.readTree(expected_level_2);
      assertThat(secondResponse)
          .isNotNull()
          .satisfies(
              secondResponseNode -> {
                assertThat(secondResponseNode.document())
                    .usingRecursiveComparison()
                    .ignoringFields("order")
                    .isEqualTo(expectedData2);
                assertThat(secondResponseNode.modified())
                    .isEqualTo(true); // modified $vectorize and $vector
              });
    }

    @Test
    public void update_vector_at_first_level() throws Exception {
      String updateVectorizeData =
          """
                                  {"$vectorize" : "Beijing is a big city", "$vector" : [0.2,0.4,0.5]}
                                  """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, (ObjectNode) objectMapper.readTree(updateVectorizeData)));
      String expected_level_1 =
          """
                      {
                          "_id": "1",
                          "location": "London",
                          "$vector": [0.2,0.4,0.5]
                      }
                    """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON); // location as London
      JsonNode expectedData1 = objectMapper.readTree(expected_level_1);
      DocumentUpdater.DocumentUpdaterResponse firstResponse =
          documentUpdater.apply(baseData, false);
      assertThat(firstResponse)
          .isNotNull()
          .satisfies(
              firstResponseNode -> {
                assertThat(firstResponseNode.document())
                    .usingRecursiveComparison()
                    .ignoringFields("order")
                    .isEqualTo(expectedData1);
                assertThat(firstResponseNode.modified()).isEqualTo(true); // vector is modified
              });

      // Second level update will vectorize in setOperation
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      final DocumentUpdater.DocumentUpdaterResponse secondResponse =
          documentUpdater
              .applyUpdateVectorize(firstResponse.document(), false, dataVectorizer)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      String expected_level_2 =
          """
                      {
                          "_id":"1",
                          "location": "London",
                          "$vectorize" : "Beijing is a big city",
                          "$vector": [0.25,0.25,0.25]
                      }
                      """;
      JsonNode expectedData2 = objectMapper.readTree(expected_level_2);
      assertThat(secondResponse)
          .isNotNull()
          .satisfies(
              secondResponseNode -> {
                assertThat(secondResponseNode.document())
                    .usingRecursiveComparison()
                    .ignoringFields("order")
                    .isEqualTo(expectedData2);
                assertThat(secondResponseNode.modified())
                    .isEqualTo(true); // modified $vectorize and $vector
              });
    }

    @Test
    public void two_levels_update_unset() throws Exception {
      String updateVectorizeData =
          """
                                        {"$vectorize" : "Beijing is a big city", "location" : "London"}
                                        """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.UNSET,
                  (ObjectNode)
                      objectMapper.readTree(
                          updateVectorizeData))); // will unset $vectorize, $vector and location

      String expected_level_1 =
          """
                                  {
                                      "_id": "1"
                                  }
                                """;

      JsonNode baseData = objectMapper.readTree(BASE_DOC_JSON_VECTOR);
      JsonNode expectedData1 = objectMapper.readTree(expected_level_1);
      DocumentUpdater.DocumentUpdaterResponse firstResponse =
          documentUpdater.apply(baseData, false);
      assertThat(firstResponse)
          .isNotNull()
          .satisfies(
              firstResponseNode -> {
                assertThat(firstResponseNode.document()).isEqualTo(expectedData1);
                assertThat(firstResponseNode.modified())
                    .isEqualTo(true); // modified $vectorize, $vector and location
              });

      // Second level update will try to vectorize, in this test case, will do nothing, since there
      // is no setOperation
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      final DocumentUpdater.DocumentUpdaterResponse secondResponse =
          documentUpdater
              .applyUpdateVectorize(firstResponse.document(), false, dataVectorizer)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      String expected_level_2 =
          """
                                  {
                                      "_id":"1"
                                  }
                                  """;
      JsonNode expectedData2 = objectMapper.readTree(expected_level_2);
      assertThat(secondResponse)
          .isNotNull()
          .satisfies(
              secondResponseNode -> {
                assertThat(secondResponseNode.document())
                    .usingRecursiveComparison()
                    .ignoringFields("order")
                    .isEqualTo(expectedData2);
                assertThat(secondResponseNode.modified()).isEqualTo(false); // nothing is modified
              });
    }
  }
}
