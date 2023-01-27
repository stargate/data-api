package io.stargate.sgv3.docsapi.service.updater;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv3.docsapi.service.testutil.DocumentUpdaterUtils;
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
      JsonNode updatedDocument = documentUpdater.applyUpdates(baseData);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node).isEqualTo(expectedData);
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
      JsonNode updatedDocument = documentUpdater.applyUpdates(baseData);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node).isEqualTo(expectedData);
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
      JsonNode updatedDocument = documentUpdater.applyUpdates(baseData);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node).isEqualTo(expectedData);
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
      JsonNode updatedDocument = documentUpdater.applyUpdates(baseData);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node).isEqualTo(expectedData);
              });
    }
  }

  @Nested
  class UpdateDocumentInvalid {
    /** Test for ensuring it is not legal to "$set" document id (_id) */
    /*
    @Test
    public void invalidSetDocId() throws Exception {
      JsonNode baseDoc = objectMapper.readTree(BASE_DOC_JSON);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET,
                  objectMapper.getNodeFactory().objectNode().put("_id", "xyz")));
      Throwable t = catchThrowable(() -> documentUpdater.applyUpdates(baseDoc));
      assertThat(t)
          .isNotNull()
          .withFailMessage("Should throw exception on $set of _id")
          .isInstanceOf(DocsException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID)
          .hasMessage(ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID + ": $set");
    }
     */
  }
}
