package io.stargate.sgv2.jsonapi.service.updater;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import javax.inject.Inject;
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
          documentUpdater.applyUpdates(baseData, false);
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
          documentUpdater.applyUpdates(baseData, false);
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
          documentUpdater.applyUpdates(baseData, false);
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
          documentUpdater.applyUpdates(baseData, false);
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
      String updateClause = """
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
          .hasMessageStartingWith("Invalid update operator 'location' (must start with '$')");
    }

    @Test
    public void unsupportedUpdateOperator() throws Exception {
      String updateClause = """
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
          .hasMessageStartingWith("Unsupported update operator '$pullAll'");
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
}
