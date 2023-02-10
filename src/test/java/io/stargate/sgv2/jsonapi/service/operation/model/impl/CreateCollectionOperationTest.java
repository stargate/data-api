package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CreateCollectionOperationTest extends AbstractValidatingStargateBridgeTest {
  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  private CommandContext commandContext = new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  @Inject QueryExecutor queryExecutor;

  @Nested
  class CreateCollectionOperationsTest {

    @Test
    public void createCollection() throws Exception {
      List<String> queries = getAllQueryString(KEYSPACE_NAME, COLLECTION_NAME);
      queries.stream().forEach(query -> withQuery(query).returningNothing());

      CreateCollectionOperation createCollectionOperation =
          new CreateCollectionOperation(commandContext, COLLECTION_NAME);

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

  private List<String> getAllQueryString(String database, String collection) {
    List<String> queries = new ArrayList<>();
    String create =
        "CREATE TABLE IF NOT EXISTS %s.%s ("
            + "    key                 tuple<tinyint,text>,"
            + "    tx_id               timeuuid, "
            + "    doc_json            text,"
            + "    doc_properties      map<text, int>,"
            + "    exist_keys          set<text>,"
            + "    sub_doc_equals      set<text>,"
            + "    array_size          map<text, int>,"
            + "    array_equals        map<text, text>,"
            + "    array_contains      set<text>,"
            + "    query_bool_values   map<text, tinyint>,"
            + "    query_dbl_values    map<text, decimal>,"
            + "    query_text_values   map<text, text>, "
            + "    query_null_values   set<text>,     "
            + "    PRIMARY KEY (key))";
    queries.add(create.formatted(database, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_doc_properties ON %s.%s (entries(doc_properties)) USING 'StorageAttachedIndex'"
            .formatted(collection, database, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_exists_keys ON %s.%s (exist_keys) USING 'StorageAttachedIndex'"
            .formatted(collection, database, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_sub_doc_equals ON %s.%s (sub_doc_equals) USING 'StorageAttachedIndex'"
            .formatted(collection, database, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_size ON %s.%s (entries(array_size)) USING 'StorageAttachedIndex'"
            .formatted(collection, database, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_equals ON %s.%s (entries(array_equals)) USING 'StorageAttachedIndex'"
            .formatted(collection, database, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_contains ON %s.%s (array_contains) USING 'StorageAttachedIndex'"
            .formatted(collection, database, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_bool_values ON %s.%s (entries(query_bool_values)) USING 'StorageAttachedIndex'"
            .formatted(collection, database, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_dbl_values ON %s.%s (entries(query_dbl_values)) USING 'StorageAttachedIndex'"
            .formatted(collection, database, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_text_values ON %s.%s (entries(query_text_values)) USING 'StorageAttachedIndex'"
            .formatted(collection, database, collection));
    queries.add(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_null_values ON %s.%s (query_null_values) USING 'StorageAttachedIndex'"
            .formatted(collection, database, collection));
    return queries;
  }
}
