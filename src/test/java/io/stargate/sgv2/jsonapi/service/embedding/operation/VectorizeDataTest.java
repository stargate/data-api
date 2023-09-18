package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.VectorizeData;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(EmbeddingServiceCacheTest.PropertyBasedOverrideProfile.class)
public class VectorizeDataTest {
  @Inject ObjectMapper objectMapper;

  CommandContext commandContext = CommandContext.empty();
  private EmbeddingService testService = new TestEmbeddingService();

  @Nested
  public class TestTextValues {

    @Test
    public void testTextValues() {
      List<JsonNode> documents = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        documents.add(objectMapper.createObjectNode().put("$vectorize", "test data"));
      }
      VectorizeData vectorizeData = new VectorizeData(testService, objectMapper.getNodeFactory());
      vectorizeData.vectorize(documents);
      for (JsonNode document : documents) {
        assertThat(document.has("$vectorize")).isTrue();
        assertThat(document.has("$vector")).isTrue();
        assertThat(document.get("$vector").isArray()).isTrue();
        assertThat(document.get("$vector").size()).isEqualTo(3);
      }
    }

    @Test
    public void testNonTextValues() {
      List<JsonNode> documents = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        documents.add(objectMapper.createObjectNode().put("$vectorize", 5));
      }

      VectorizeData vectorizeData = new VectorizeData(testService, objectMapper.getNodeFactory());
      Throwable failure = catchThrowable(() -> vectorizeData.vectorize(documents));
      assertThat(failure)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_VECTORIZE_VALUE)
          .hasFieldOrPropertyWithValue("message", "$vectorize search needs to be text value");
    }

    @Test
    public void testNullValues() {
      List<JsonNode> documents = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        documents.add(objectMapper.createObjectNode().put("$vectorize", (String) null));
      }

      VectorizeData vectorizeData = new VectorizeData(testService, objectMapper.getNodeFactory());
      vectorizeData.vectorize(documents);
      for (JsonNode document : documents) {
        assertThat(document.has("$vectorize")).isTrue();
        assertThat(document.has("$vector")).isTrue();
      }
    }
  }

  @Nested
  public class SortClauseValues {
    @Test
    public void sortClauseValues() {
      SortExpression sortExpression = SortExpression.vectorizeSearch("test data");
      List<SortExpression> sortExpressions = new ArrayList<>();
      sortExpressions.add(sortExpression);
      SortClause sortClause = new SortClause(sortExpressions);
      VectorizeData vectorizeData = new VectorizeData(testService, objectMapper.getNodeFactory());
      vectorizeData.vectorize(sortClause);
      assertThat(sortClause.hasVsearchClause()).isTrue();
      assertThat(sortClause.hasVectorizeSearchClause()).isFalse();
      assertThat(sortClause.sortExpressions().get(0).vector()).isNotNull();
      assertThat(sortClause.sortExpressions().get(0).vector().length).isEqualTo(3);
    }
  }

  @Nested
  public class UpdateClauseValues {
    @Test
    public void updateClauseSetValues() throws Exception {
      String json =
          """
        {
          "findOneAndUpdate": {
            "filter" : {"_id" : "id"},
            "update" : {"$set" : {"$vectorize" : "New York"}}
          }
        }
        """;
      FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
      UpdateClause updateClause = command.updateClause();
      VectorizeData vectorizeData = new VectorizeData(testService, objectMapper.getNodeFactory());
      vectorizeData.vectorizeUpdateClause(updateClause);
      final ObjectNode setNode = updateClause.updateOperationDefs().get(UpdateOperator.SET);
      assertThat(setNode.has("$vectorize")).isTrue();
      assertThat(setNode.has("$vector")).isTrue();
      assertThat(setNode.get("$vector").isArray()).isTrue();
      assertThat(setNode.get("$vector").size()).isEqualTo(3);
    }

    @Test
    public void updateClauseSetOnInsertValues() throws Exception {
      String json =
          """
        {
          "findOneAndUpdate": {
            "filter" : {"_id" : "id"},
            "update" : {"$setOnInsert" : {"$vectorize" : "New York"}}
          }
        }
        """;
      FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
      UpdateClause updateClause = command.updateClause();
      VectorizeData vectorizeData = new VectorizeData(testService, objectMapper.getNodeFactory());
      vectorizeData.vectorizeUpdateClause(updateClause);
      final ObjectNode setNode =
          updateClause.updateOperationDefs().get(UpdateOperator.SET_ON_INSERT);
      assertThat(setNode.has("$vectorize")).isTrue();
      assertThat(setNode.has("$vector")).isTrue();
      assertThat(setNode.get("$vector").isArray()).isTrue();
      assertThat(setNode.get("$vector").size()).isEqualTo(3);
    }

    @Test
    public void updateClauseUnsetValues() throws Exception {
      String json =
          """
        {
          "findOneAndUpdate": {
            "filter" : {"_id" : "id"},
            "update" : {"$unset" : {"$vectorize" : null}}
          }
        }
        """;
      FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
      UpdateClause updateClause = command.updateClause();
      VectorizeData vectorizeData = new VectorizeData(testService, objectMapper.getNodeFactory());
      vectorizeData.vectorizeUpdateClause(updateClause);
      final ObjectNode unsetNode = updateClause.updateOperationDefs().get(UpdateOperator.UNSET);
      assertThat(unsetNode.has("$vectorize")).isTrue();
      assertThat(unsetNode.has("$vector")).isTrue();
      assertThat(unsetNode.get("$vector").isNull()).isTrue();
    }
  }
}
