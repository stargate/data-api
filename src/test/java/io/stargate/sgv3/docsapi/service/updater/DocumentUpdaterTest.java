package io.stargate.sgv3.docsapi.service.updater;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperator;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DocumentUpdaterTest {
  @Inject ObjectMapper objectMapper;

  @Nested
  class UpdateDocument {

    @Test
    public void setUpdateCondition() throws Exception {
      String json =
          """
                      {
                          "_id": "1",
                          "location": "London"
                      }
                    """;

      String expected =
          """
                      {
                          "_id": "1",
                          "location": "New York"
                      }
                    """;

      JsonNode baseData = objectMapper.readTree(json);
      JsonNode expectedData = objectMapper.readTree(expected);
      DocumentUpdater documentUpdater =
          new DocumentUpdater(
              new UpdateClause(
                  List.of(
                      new UpdateOperation(
                          "location",
                          UpdateOperator.SET,
                          objectMapper.getNodeFactory().textNode("New York")))));
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
      String json =
          """
                          {
                              "_id": "1",
                              "location": "London"
                          }
                        """;

      String expected =
          """
                          {
                              "_id": "1",
                              "location": "London",
                              "new_data" : "data"
                          }
                        """;

      JsonNode baseData = objectMapper.readTree(json);
      JsonNode expectedData = objectMapper.readTree(expected);
      DocumentUpdater documentUpdater =
          new DocumentUpdater(
              new UpdateClause(
                  List.of(
                      new UpdateOperation(
                          "new_data",
                          UpdateOperator.SET,
                          objectMapper.getNodeFactory().textNode("data")))));
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
      String json =
          """
                              {
                                  "_id": "1",
                                  "location": "London"
                              }
                            """;

      String expected =
          """
                              {
                                  "_id": "1",
                                  "location": "London",
                                  "new_data" : 40
                              }
                            """;

      JsonNode baseData = objectMapper.readTree(json);
      JsonNode expectedData = objectMapper.readTree(expected);
      DocumentUpdater documentUpdater =
          new DocumentUpdater(
              new UpdateClause(
                  List.of(
                      new UpdateOperation(
                          "new_data",
                          UpdateOperator.SET,
                          objectMapper.getNodeFactory().numberNode(40)))));
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
      String json =
          """
                              {
                                  "_id": "1",
                                  "location": "London",
                                  "col": "data"
                              }
                            """;

      String expected =
          """
                              {
                                  "_id": "1",
                                  "location": "London"
                              }
                            """;

      JsonNode baseData = objectMapper.readTree(json);
      JsonNode expectedData = objectMapper.readTree(expected);
      DocumentUpdater documentUpdater =
          new DocumentUpdater(
              new UpdateClause(
                  List.of(
                      new UpdateOperation(
                          "col",
                          UpdateOperator.UNSET,
                          objectMapper.getNodeFactory().textNode("")))));
      JsonNode updatedDocument = documentUpdater.applyUpdates(baseData);
      assertThat(updatedDocument)
          .isNotNull()
          .satisfies(
              node -> {
                assertThat(node).isEqualTo(expectedData);
              });
    }
  }
}
