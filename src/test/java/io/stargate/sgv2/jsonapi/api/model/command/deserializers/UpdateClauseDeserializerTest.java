package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.SetOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperation;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UpdateClauseDeserializerTest {

  @Inject ObjectMapper objectMapper;

  @Nested
  class UpdateDeserializer {

    @Test
    public void happyPathUpdateOperation() throws Exception {
      String json =
          """
                    {"$set" : {"username": "aaron", "number" : 40}}
                    """;

      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      final UpdateOperation operation =
          SetOperation.constructSet(
              objectMapper
                  .getNodeFactory()
                  .objectNode()
                  .put("username", "aaron")
                  .put("number", 40));
      assertThat(updateClause).isNotNull();
      assertThat(updateClause.buildOperations()).hasSize(1).contains(operation);
    }

    @Test
    public void mustHandleNull() throws Exception {
      String json = "null";

      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause).isNull();
    }

    @Test
    public void mustHandleEmpty() throws Exception {
      String json =
          """
                    {}
                    """;

      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause.buildOperations()).hasSize(0);
    }

    @Test
    public void mustHandleString() throws Exception {
      String json =
          """
                    {"$set" : {"username": "aaron"}}
                    """;
      final UpdateOperation operation =
          SetOperation.constructSet(
              objectMapper.getNodeFactory().objectNode().put("username", "aaron"));
      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause.buildOperations()).hasSize(1).contains(operation);
    }

    @Test
    public void mustHandleNumber() throws Exception {
      String json =
          """
                    {"$set" : {"numberType": 40}}
                    """;
      final UpdateOperation operation =
          SetOperation.constructSet(
              objectMapper.getNodeFactory().objectNode().put("numberType", 40));
      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause.buildOperations()).hasSize(1).contains(operation);
    }

    @Test
    public void mustHandleBoolean() throws Exception {
      String json =
          """
                    {"$set" : {"boolType": true}}
                    """;
      final UpdateOperation operation =
          SetOperation.constructSet(
              objectMapper.getNodeFactory().objectNode().put("boolType", true));
      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause.buildOperations()).hasSize(1).contains(operation);
    }

    @Test
    public void mustHandleArray() throws Exception {
      String json =
          """
                    {"$set" : {"arrayType": ["a"]}}
                    """;

      final UpdateOperation operation =
          SetOperation.constructSet((ObjectNode) objectMapper.readTree("{\"arrayType\": [\"a\"]}"));
      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause.buildOperations()).hasSize(1).contains(operation);
    }

    @Test
    public void mustHandleSubdoc() throws Exception {
      String json =
          """
                    {"$set" : {"subDocType": {"sub_doc_col" : "sub_doc_val"}}}}
                    """;

      final UpdateOperation operation =
          SetOperation.constructSet(
              (ObjectNode)
                  objectMapper.readTree("{\"subDocType\": {\"sub_doc_col\": \"sub_doc_val\"}}"));
      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause.buildOperations()).hasSize(1).contains(operation);
    }

    @Test
    public void mustHandleDate() throws Exception {
      String json =
          """
                {"$set" : {"dateType": {"$date": 123456}}}
                """;
      final UpdateOperation operation =
          SetOperation.constructSet(
              (ObjectNode)
                  objectMapper.readTree(
                      """
                              {
                                "dateType": {
                                  "$date": 123456
                                }
                              }
                              """));
      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause.buildOperations()).hasSize(1).contains(operation);
    }
  }
}
