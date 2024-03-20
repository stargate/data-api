package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizer;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PropertyBasedOverrideProfile.class)
public class DataVectorizerTest {
  @Inject ObjectMapper objectMapper;

  CommandContext commandContext = CommandContext.empty();
  private EmbeddingProvider testService = new TestEmbeddingProvider();
  private CollectionSettings collectionSettings =
      TestEmbeddingProvider.commandContextWithVectorize.collectionSettings();

  @Nested
  public class TestTextValues {

    @Test
    public void testTextValues() {
      List<JsonNode> documents = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        documents.add(objectMapper.createObjectNode().put("$vectorize", "test data"));
      }
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      try {
        dataVectorizer.vectorize(documents).subscribe().asCompletionStage().get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
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

      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      try {
        Throwable failure =
            dataVectorizer
                .vectorize(documents)
                .subscribe()
                .withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();
        assertThat(failure)
            .isInstanceOf(JsonApiException.class)
            .hasFieldOrPropertyWithValue("errorCode", ErrorCode.INVALID_VECTORIZE_VALUE_TYPE)
            .hasFieldOrPropertyWithValue(
                "message",
                "$vectorize value needs to be text value, issue in document at position 1");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Test
    public void testNullValues() {
      List<JsonNode> documents = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        documents.add(objectMapper.createObjectNode().put("$vectorize", (String) null));
      }

      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      try {
        dataVectorizer.vectorize(documents).subscribe().asCompletionStage().get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      for (JsonNode document : documents) {
        assertThat(document.has("$vectorize")).isTrue();
        assertThat(document.has("$vector")).isTrue();
      }
    }

    @Test
    public void testWithBothVectorFieldValues() {
      List<JsonNode> documents = new ArrayList<>();

      final ObjectNode document = objectMapper.createObjectNode().put("$vectorize", "test data");
      final ArrayNode arrayNode = document.putArray("$vector");
      arrayNode.add(objectMapper.getNodeFactory().numberNode(0.11f));
      arrayNode.add(objectMapper.getNodeFactory().numberNode(0.11f));
      documents.add(document);
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      try {
        Throwable failure =
            dataVectorizer
                .vectorize(documents)
                .subscribe()
                .withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();
        assertThat(failure)
            .isNotNull()
            .isInstanceOf(JsonApiException.class)
            .withFailMessage(
                "$vectorize` and `$vector` can't be used together, issue in document at position 1")
            .hasFieldOrPropertyWithValue("errorCode", ErrorCode.INVALID_USAGE_OF_VECTORIZE);
      } catch (Exception e) {
        throw new RuntimeException(e);
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
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      try {
        dataVectorizer.vectorize(sortClause).subscribe().asCompletionStage().get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
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
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      try {
        dataVectorizer.vectorizeUpdateClause(updateClause).subscribe().asCompletionStage().get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      final ObjectNode setNode = updateClause.updateOperationDefs().get(UpdateOperator.SET);
      assertThat(setNode.has("$vectorize")).isTrue();
      assertThat(setNode.has("$vector")).isTrue();
      assertThat(setNode.get("$vector").isArray()).isTrue();
      assertThat(setNode.get("$vector").size()).isEqualTo(3);
    }

    @Test
    public void updateClauseSetBothValues() throws Exception {
      String json =
          """
            {
              "findOneAndUpdate": {
                "filter" : {"_id" : "id"},
                "update" : {"$set" : {"$vectorize" : "New York", "$vector" : [0.11, 0.11]}}
              }
            }
            """;
      FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
      UpdateClause updateClause = command.updateClause();
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      Throwable t =
          dataVectorizer
              .vectorizeUpdateClause(updateClause)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(t)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .withFailMessage("`$vectorize` and `$vector` can't be used together.")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.INVALID_USAGE_OF_VECTORIZE)
          .hasMessage(ErrorCode.INVALID_USAGE_OF_VECTORIZE.getMessage());
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
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      try {
        dataVectorizer.vectorizeUpdateClause(updateClause).subscribe().asCompletionStage().get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      final ObjectNode setNode =
          updateClause.updateOperationDefs().get(UpdateOperator.SET_ON_INSERT);
      assertThat(setNode.has("$vectorize")).isTrue();
      assertThat(setNode.has("$vector")).isTrue();
      assertThat(setNode.get("$vector").isArray()).isTrue();
      assertThat(setNode.get("$vector").size()).isEqualTo(3);
    }

    @Test
    public void updateClauseSetOnInsertBothValues() throws Exception {
      String json =
          """
                      {
                        "findOneAndUpdate": {
                          "filter" : {"_id" : "id"},
                          "update" : {"$setOnInsert" : {"$vectorize" : "New York", "$vector" : [0.11, 0.11]}}
                        }
                      }
                      """;
      FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
      UpdateClause updateClause = command.updateClause();
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      Throwable t =
          dataVectorizer
              .vectorizeUpdateClause(updateClause)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(t)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .withFailMessage("`$vectorize` and `$vector` can't be used together.")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.INVALID_USAGE_OF_VECTORIZE)
          .hasMessage(ErrorCode.INVALID_USAGE_OF_VECTORIZE.getMessage());
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
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      try {
        dataVectorizer.vectorizeUpdateClause(updateClause).subscribe().asCompletionStage().get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      final ObjectNode unsetNode = updateClause.updateOperationDefs().get(UpdateOperator.UNSET);
      assertThat(unsetNode.has("$vectorize")).isTrue();
      assertThat(unsetNode.has("$vector")).isTrue();
      assertThat(unsetNode.get("$vector").isNull()).isTrue();
    }

    @Test
    public void updateClauseUnsetBothValues() throws Exception {
      String json =
          """
                      {
                        "findOneAndUpdate": {
                          "filter" : {"_id" : "id"},
                          "update" : {"$unset" : {"$vectorize" : null, "$vector" : null}}
                        }
                      }
                      """;
      FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
      UpdateClause updateClause = command.updateClause();
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), Optional.empty(), collectionSettings);
      try {
        Throwable t =
            dataVectorizer
                .vectorizeUpdateClause(updateClause)
                .subscribe()
                .withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();

        assertThat(t)
            .isNotNull()
            .isInstanceOf(JsonApiException.class)
            .withFailMessage("`$vectorize` and `$vector` can't be used together.")
            .hasFieldOrPropertyWithValue("errorCode", ErrorCode.INVALID_USAGE_OF_VECTORIZE)
            .hasMessage(ErrorCode.INVALID_USAGE_OF_VECTORIZE.getMessage());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
