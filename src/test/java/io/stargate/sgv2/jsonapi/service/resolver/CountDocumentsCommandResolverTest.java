package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CountCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.MapCollectionFilter.Operator;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.TextCollectionFilter;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class CountDocumentsCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject CountDocumentsCommandResolver resolver;

  @Inject OperationsConfig operationsConfig;
  @InjectMock protected DataApiRequestInfo dataApiRequestInfo;

  @Nested
  class ResolveCommand {

    CommandContext<CollectionSchemaObject> commandContext = TestConstants.COLLECTION_CONTEXT;

    @Test
    public void noFilter() throws Exception {
      String json =
          """
            {
              "countDocuments": {
              }
            }
            """;

      CountDocumentsCommand command = objectMapper.readValue(json, CountDocumentsCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CountCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.logicalExpression().comparisonExpressions).isEmpty();
                assertThat(op.pageSize()).isEqualTo(operationsConfig.defaultCountPageSize());
                assertThat(op.limit()).isEqualTo(operationsConfig.maxCountLimit());
              });
    }

    @Test
    public void withFilter() throws Exception {
      String json =
          """
            {
              "countDocuments": {
                "filter": {
                    "name": "Aaron"
                }
              }
            }
            """;

      CountDocumentsCommand command = objectMapper.readValue(json, CountDocumentsCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CountCollectionOperation.class,
              op -> {
                TextCollectionFilter expected =
                    new TextCollectionFilter("name", Operator.EQ, "Aaron");

                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(
                        op.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(expected);
                assertThat(op.pageSize()).isEqualTo(operationsConfig.defaultCountPageSize());
                assertThat(op.limit()).isEqualTo(operationsConfig.maxCountLimit());
              });
    }
  }
}