package io.stargate.sgv2.jsonapi.service.processor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataSuplier;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;

/** tests data and mocks for working with {@link CommandContext} */
public class CommandContextTestData extends TestDataSuplier {

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  public CommandContextTestData(TestData testData) {
    super(testData);
  }

  public CommandContext<TableSchemaObject> tableSchemaObjectCommandContext(
      TableSchemaObject tableSchemaObject) {
    return CommandContext.builderSupplier()
        .withJsonProcessingMetricsReporter(mock(JsonProcessingMetricsReporter.class))
        .withCqlSessionCache(mock(CQLSessionCache.class))
        .withCommandConfig(new CommandConfig())
        .withEmbeddingProviderFactory(mock(EmbeddingProviderFactory.class))
        .withRerankingProviderFactory(mock(RerankingProviderFactory.class))
        .withMeterRegistry(mock(MeterRegistry.class))
        .getBuilder(tableSchemaObject)
        .withEmbeddingProvider(null)
        .withCommandName("test-command")
        .withRequestContext(TEST_CONSTANTS.requestContext())
        .withApiFeatures(ApiFeatures.empty())
        .build();
  }

  public <T extends SchemaObject> CommandContext<T> mockCommandContext(T schemaObject) {

    CommandContext<T> commandContextMock = mock(CommandContext.class);

    when(commandContextMock.schemaObject()).thenReturn(schemaObject);
    when(commandContextMock.asDatabaseContext())
        .thenReturn((CommandContext<DatabaseSchemaObject>) commandContextMock);
    when(commandContextMock.asKeyspaceContext())
        .thenReturn((CommandContext<KeyspaceSchemaObject>) commandContextMock);
    when(commandContextMock.asCollectionContext())
        .thenReturn((CommandContext<CollectionSchemaObject>) commandContextMock);
    when(commandContextMock.asTableContext())
        .thenReturn((CommandContext<TableSchemaObject>) commandContextMock);
    return commandContextMock;
  }
}
