package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizer;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankingConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
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
  private final EmbeddingProvider testService = new TestEmbeddingProvider();
  private final CollectionSchemaObject collectionSettings =
      TestEmbeddingProvider.commandContextWithVectorize.schemaObject();
  private final EmbeddingCredentials embeddingCredentials =
      new EmbeddingCredentials(Optional.empty(), Optional.empty(), Optional.empty());

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
              testService, objectMapper.getNodeFactory(), embeddingCredentials, collectionSettings);
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
    public void testEmptyValues() {
      List<JsonNode> documents = new ArrayList<>();
      for (int i = 0; i <= 3; i++) {
        if (i % 2 == 0) {
          documents.add(objectMapper.createObjectNode().put("$vectorize", ""));
        } else {
          documents.add(objectMapper.createObjectNode().put("$vectorize", "test data"));
        }
      }

      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), embeddingCredentials, collectionSettings);
      try {
        dataVectorizer.vectorize(documents).subscribe().asCompletionStage().get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      for (int i = 0; i <= 3; i++) {
        JsonNode document = documents.get(i);
        if (i % 2 == 0) {
          assertThat(document.has("$vectorize")).isTrue();
          assertThat(document.has("$vector")).isTrue();
          assertThat(document.get("$vector").isNull()).isTrue();
        } else {
          assertThat(document.has("$vectorize")).isTrue();
          assertThat(document.has("$vector")).isTrue();
          assertThat(document.get("$vector").isArray()).isTrue();
          assertThat(document.get("$vector").size()).isEqualTo(3);
        }
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
              testService, objectMapper.getNodeFactory(), embeddingCredentials, collectionSettings);
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
            .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.INVALID_VECTORIZE_VALUE_TYPE)
            .hasFieldOrPropertyWithValue(
                "message",
                "$vectorize value needs to be text value: issue in document at position 1");
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
              testService, objectMapper.getNodeFactory(), embeddingCredentials, collectionSettings);
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
              testService, objectMapper.getNodeFactory(), embeddingCredentials, collectionSettings);
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
            .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.INVALID_USAGE_OF_VECTORIZE);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Test
    public void testWithUnmatchedVectorsNumber() {
      TestEmbeddingProvider testProvider =
          new TestEmbeddingProvider() {
            @Override
            public Uni<Response> vectorize(
                int batchId,
                List<String> texts,
                EmbeddingCredentials embeddingCredentials,
                EmbeddingRequestType embeddingRequestType) {
              List<float[]> customResponse = new ArrayList<>();
              texts.forEach(t -> customResponse.add(new float[] {0.5f, 0.5f, 0.5f}));
              // add additional vector
              customResponse.add(new float[] {0.5f, 0.5f, 0.5f});
              return Uni.createFrom().item(Response.of(batchId, customResponse));
            }
          };
      List<JsonNode> documents = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        documents.add(objectMapper.createObjectNode().put("$vectorize", "test data"));
      }
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testProvider,
              objectMapper.getNodeFactory(),
              embeddingCredentials,
              collectionSettings);

      Throwable failure =
          dataVectorizer
              .vectorize(documents)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(failure)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue(
              "errorCode", ErrorCodeV1.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider returned an unexpected response: Embedding provider 'custom' didn't return the expected number of embeddings. Expect: '2'. Actual: '3'");
    }

    @Test
    public void testWithUnmatchedVectorSize() {
      // new collection settings with different expected vector size
      CollectionSchemaObject collectionSettings =
          new CollectionSchemaObject(
              "namespace",
              "collections",
              null,
              IdConfig.defaultIdConfig(),
              VectorConfig.fromColumnDefinitions(
                  List.of(
                      new VectorColumnDefinition(
                          DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
                          4,
                          SimilarityFunction.COSINE,
                          EmbeddingSourceModel.OTHER,
                          new VectorizeDefinition("custom", "custom", null, null)))),
              null,
              CollectionLexicalConfig.configForDisabled(),
              CollectionRerankingConfig.configForLegacyCollections());
      List<JsonNode> documents = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        documents.add(objectMapper.createObjectNode().put("$vectorize", "test data"));
      }
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), embeddingCredentials, collectionSettings);

      Throwable failure =
          dataVectorizer
              .vectorize(documents)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(failure)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue(
              "errorCode", ErrorCodeV1.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider returned an unexpected response: Embedding provider 'custom' did not return expected embedding length. Expect: '4'. Actual: '3'");
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
              testService, objectMapper.getNodeFactory(), embeddingCredentials, collectionSettings);
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
  public class vectorizeText {
    @Test
    public void vectorize() {
      final ObjectNode document = objectMapper.createObjectNode().put("$vectorize", "test data");
      final ArrayNode arrayNode = document.putArray("$vector");
      arrayNode.add(objectMapper.getNodeFactory().numberNode(0.11f));
      arrayNode.add(objectMapper.getNodeFactory().numberNode(0.11f));
      DataVectorizer dataVectorizer =
          new DataVectorizer(
              testService, objectMapper.getNodeFactory(), embeddingCredentials, collectionSettings);
      try {
        final float[] testData =
            dataVectorizer.vectorize("test data").subscribe().asCompletionStage().get();
        assertThat(testData[0]).isEqualTo(0.25f);
        assertThat(testData[1]).isEqualTo(0.25f);
        assertThat(testData[2]).isEqualTo(0.25f);

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
