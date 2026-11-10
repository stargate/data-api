package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FindAndRerankCommandResolver}: the order of the errors. The checks themselves
 * are tested in FindAndRerankSortParserTest and FindAndRerankPlannerTest.
 */
class FindAndRerankCommandResolverTest {

  /** A sort with a syntax error and no options, so the planner would reject the options too. */
  private static final String BAD_SORT_NO_OPTIONS =
      """
      {"findAndRerank": {"sort": {"age": 2}}}
      """;

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final FindAndRerankCommandResolver resolver =
      new FindAndRerankCommandResolver(
          mock(OperationsConfig.class),
          objectMapper,
          mock(MeterRegistry.class),
          mock(JsonApiMetricsConfig.class),
          mock(FindCommandResolver.class));

  /** The sort is parsed before the planner checks the options, such as the missing rerankOn. */
  @Test
  void reportsSortSyntaxErrorBeforeOptionErrors() throws Exception {
    var command = objectMapper.readValue(BAD_SORT_NO_OPTIONS, FindAndRerankCommand.class);
    var commandContext = commandContext(true);

    var ex =
        assertThrowsExactly(
            SortException.class, () -> resolver.resolveCollectionCommand(commandContext, command));
    assertThat(ex.code).isEqualTo(SortException.Code.SORT_CLAUSE_VALUE_INVALID.name());
  }

  /** A $hybrid sort with no value is a sort error, so it is reported before the missing options. */
  @Test
  void reportsHybridSortWithNoValueBeforeOptionErrors() throws Exception {
    var command =
        objectMapper.readValue(
            """
            {"findAndRerank": {"sort": {"$hybrid": ""}}}
            """,
            FindAndRerankCommand.class);
    var commandContext = commandContext(true);

    var ex =
        assertThrowsExactly(
            SortException.class, () -> resolver.resolveCollectionCommand(commandContext, command));
    assertThat(ex.code).isEqualTo(SortException.Code.MISSING_HYBRID_SORT_VALUE.name());
  }

  /** Reading the command does not check the sort, so a sort that is not an object fails here. */
  @Test
  void reportsSortThatIsNotAnObject() throws Exception {
    var command =
        objectMapper.readValue(
            """
            {"findAndRerank": {"sort": "cheese"}}
            """,
            FindAndRerankCommand.class);
    var commandContext = commandContext(true);

    var ex =
        assertThrowsExactly(
            SortException.class, () -> resolver.resolveCollectionCommand(commandContext, command));
    assertThat(ex.code).isEqualTo(SortException.Code.SORT_CLAUSE_INVALID.name());
  }

  /** The reranking feature is checked before the sort is parsed. */
  @Test
  void reportsRerankingDisabledBeforeSortSyntaxError() throws Exception {
    var command = objectMapper.readValue(BAD_SORT_NO_OPTIONS, FindAndRerankCommand.class);
    var commandContext = commandContext(false);

    var ex =
        assertThrowsExactly(
            SchemaException.class,
            () -> resolver.resolveCollectionCommand(commandContext, command));
    assertThat(ex.code).isEqualTo(SchemaException.Code.RERANKING_FEATURE_NOT_ENABLED.name());
  }

  private CommandContext<CollectionSchemaObject> commandContext(boolean rerankingEnabled) {
    var commandContext =
        TEST_CONSTANTS.collectionContext(
            CommandName.FIND_AND_RERANK, TEST_CONSTANTS.COLLECTION_SCHEMA_OBJECT);
    when(commandContext.requestContext().apiFeatures())
        .thenReturn(
            ApiFeatures.fromConfigAndRequest(
                Map.of(ApiFeature.RERANKING, String.valueOf(rerankingEnabled)), null));
    return commandContext;
  }
}
