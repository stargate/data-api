package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.common.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CreateCollectionOperationTest extends AbstractValidatingStargateBridgeTest {
  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  private CommandContext commandContext = new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);
  @Inject ObjectMapper objectMapper;
  @Inject SchemaManager schemaManager;
  @Inject QueryExecutor queryExecutor;

  @Nested
  @Disabled
  class CreateCollectionOperationsTest {

    SchemaManager schemaManagerMock = mock(SchemaManager.class);

    @Test
    public void createCollection() throws Exception {
      List<String> queries =
          getAllQueryString(KEYSPACE_NAME, COLLECTION_NAME, false, 0, null, null);
      queries.stream().forEach(query -> withQuery(query).returningNothing());

      when(schemaManagerMock.getKeyspaces()).thenReturn(null);

      CreateCollectionOperation createCollectionOperation =
          CreateCollectionOperation.withoutVectorSearch(
              commandContext, objectMapper, schemaManagerMock, COLLECTION_NAME);

      final Supplier<CommandResult> execute =
          createCollectionOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.OK)).isNotNull();
              });
    }

    @Test
    public void createCollectionCaseSensitive() throws Exception {
      List<String> queries =
          getAllQueryString(
              KEYSPACE_NAME.toUpperCase(), COLLECTION_NAME.toUpperCase(), false, 0, null, null);
      queries.stream().forEach(query -> withQuery(query).returningNothing());
      CommandContext commandContextUpper =
          new CommandContext(KEYSPACE_NAME.toUpperCase(), COLLECTION_NAME.toUpperCase());
      when(schemaManagerMock.getKeyspaces()).thenReturn(null);
      CreateCollectionOperation createCollectionOperation =
          CreateCollectionOperation.withoutVectorSearch(
              commandContextUpper, objectMapper, schemaManagerMock, COLLECTION_NAME.toUpperCase());

      final Supplier<CommandResult> execute =
          createCollectionOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.OK)).isNotNull();
              });
    }

    @Test
    public void createCollectionVector() throws Exception {
      List<String> queries =
          getAllQueryString(KEYSPACE_NAME, COLLECTION_NAME, true, 4, "cosine", null);
      queries.stream().forEach(query -> withQuery(query).returningNothing());
      when(schemaManagerMock.getKeyspaces()).thenReturn(null);
      CreateCollectionOperation createCollectionOperation =
          CreateCollectionOperation.withVectorSearch(
              commandContext, objectMapper, schemaManagerMock, COLLECTION_NAME, 4, "cosine", null);

      final Supplier<CommandResult> execute =
          createCollectionOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.OK)).isNotNull();
              });
    }

    @Test
    public void createCollectionVectorize() throws Exception {
      List<String> queries =
          getAllQueryString(
              KEYSPACE_NAME,
              COLLECTION_NAME,
              true,
              4,
              "cosine",
              "{\"service\":\"openai\",\"options\":{\"modelName\":\"text-embedding-ada-002\"}}");
      queries.stream().forEach(query -> withQuery(query).returningNothing());
      when(schemaManagerMock.getKeyspaces()).thenReturn(null);
      CreateCollectionOperation createCollectionOperation =
          CreateCollectionOperation.withVectorSearch(
              commandContext,
              objectMapper,
              schemaManagerMock,
              COLLECTION_NAME,
              4,
              "cosine",
              "{\"service\":\"openai\",\"options\":{\"modelName\":\"text-embedding-ada-002\"}}");

      final Supplier<CommandResult> execute =
          createCollectionOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.OK)).isNotNull();
              });
    }

    @Test
    public void createCollectionVectorDotProduct() throws Exception {
      List<String> queries =
          getAllQueryString(KEYSPACE_NAME, COLLECTION_NAME, true, 4, "dot_product", null);
      queries.stream().forEach(query -> withQuery(query).returningNothing());
      when(schemaManagerMock.getKeyspaces()).thenReturn(null);
      CreateCollectionOperation createCollectionOperation =
          CreateCollectionOperation.withVectorSearch(
              commandContext,
              objectMapper,
              schemaManagerMock,
              COLLECTION_NAME,
              4,
              "dot_product",
              null);

      final Supplier<CommandResult> execute =
          createCollectionOperation.execute(queryExecutor).subscribeAsCompletionStage().get();
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.OK)).isNotNull();
              });
    }
  }

  private List<String> getAllQueryString(
      String namespace,
      String collection,
      boolean vectorSearch,
      int vectorSize,
      String vectorFunction,
      String vectorize) {
    List<String> queries = new ArrayList<>();
    String createTable =
        "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" ("
            + "    key                 tuple<tinyint,text>,"
            + "    tx_id               timeuuid, "
            + "    doc_json            text,"
            + "    exist_keys          set<text>,"
            + "    array_size          map<text, int>,"
            + "    array_contains      set<text>,"
            + "    query_bool_values   map<text, tinyint>,"
            + "    query_dbl_values    map<text, decimal>,"
            + "    query_text_values   map<text, text>, "
            + "    query_timestamp_values map<text, timestamp>, "
            + "    query_null_values   set<text>, "
            + "    PRIMARY KEY (key))";

    String createTableWithVector =
        "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" ("
            + "    key                 tuple<tinyint,text>,"
            + "    tx_id               timeuuid, "
            + "    doc_json            text,"
            + "    exist_keys          set<text>,"
            + "    array_size          map<text, int>,"
            + "    array_contains      set<text>,"
            + "    query_bool_values   map<text, tinyint>,"
            + "    query_dbl_values    map<text, decimal>,"
            + "    query_text_values   map<text, text>, "
            + "    query_timestamp_values map<text, timestamp>, "
            + "    query_null_values   set<text>,     "
            + "    query_vector_value  VECTOR<FLOAT, "
            + vectorSize
            + ">, "
            + "    PRIMARY KEY (key))";
    if (vectorize != null) {
      createTableWithVector = createTableWithVector + " WITH comment = '" + vectorize + "'";
    }
    if (vectorSearch) {
      queries.add(String.format(createTableWithVector, namespace, collection));
    } else {
      queries.add(String.format(createTable, namespace, collection));
    }
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_exists_keys ON \"%s\".\"%s\" (exist_keys) USING 'StorageAttachedIndex'"
            .formatted(collection, namespace, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_size ON \"%s\".\"%s\" (entries(array_size)) USING 'StorageAttachedIndex'"
            .formatted(collection, namespace, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_contains ON \"%s\".\"%s\" (array_contains) USING 'StorageAttachedIndex'"
            .formatted(collection, namespace, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_bool_values ON \"%s\".\"%s\" (entries(query_bool_values)) USING 'StorageAttachedIndex'"
            .formatted(collection, namespace, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_dbl_values ON \"%s\".\"%s\" (entries(query_dbl_values)) USING 'StorageAttachedIndex'"
            .formatted(collection, namespace, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_text_values ON \"%s\".\"%s\" (entries(query_text_values)) USING 'StorageAttachedIndex'"
            .formatted(collection, namespace, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_timestamp_values ON \"%s\".\"%s\" (entries(query_timestamp_values)) USING 'StorageAttachedIndex'"
            .formatted(collection, namespace, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_null_values ON \"%s\".\"%s\" (query_null_values) USING 'StorageAttachedIndex'"
            .formatted(collection, namespace, collection));
    if (vectorSearch) {
      String vectorSearchIndex =
          "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_vector_value ON \"%s\".\"%s\" (query_vector_value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function': '"
              + vectorFunction
              + "'}";
      queries.add(vectorSearchIndex.formatted(collection, namespace, collection));
    }

    return queries;
  }
}
