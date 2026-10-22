package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.KeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentialsResolver;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.tenant.RequestTenantResolver;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectCache;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectCacheSupplier;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.util.LoggerTestWrapper;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Provider;
import java.security.Principal;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for the request-context lifetime in {@link McpResource}: one context per tool call,
 * closed exactly once when the call ends.
 */
class McpResourceTest {

  private static final TestConstants TEST_CONSTANTS = new TestConstants();

  private final ObjectMapper objectMapper = spy(new ObjectMapper());
  private final MeteredCommandProcessor commandProcessor = mock(MeteredCommandProcessor.class);
  private final SchemaObjectCache schemaObjectCache = mock(SchemaObjectCache.class);

  @Test
  void closesContextOnSuccess() throws Exception {
    var context = commandContext(true);
    var command = keyspaceCommand();
    var result = CommandResult.statusOnlyBuilder(RequestTracing.NO_OP).build();
    when(commandProcessor.processCommand(context, command))
        .thenReturn(Uni.createFrom().item(result));

    var response =
        mcpResource()
            .processCommand(Uni.createFrom().item(context), command)
            .await()
            .atMost(Duration.ofSeconds(5));

    assertThat(response.isError()).isFalse();
    verify(context, times(1)).close();
  }

  @Test
  void closesContextOnFailure() throws Exception {
    var context = commandContext(true);
    var command = keyspaceCommand();
    when(commandProcessor.processCommand(context, command))
        .thenReturn(Uni.createFrom().failure(new RuntimeException("processor failed")));

    assertThatThrownBy(
            () ->
                mcpResource()
                    .processCommand(Uni.createFrom().item(context), command)
                    .await()
                    .atMost(Duration.ofSeconds(5)))
        .hasMessage("processor failed");
    verify(context, times(1)).close();
  }

  @Test
  void closesContextWhenProcessorThrowsSynchronously() throws Exception {
    var context = commandContext(true);
    var command = keyspaceCommand();
    var failure = new IllegalStateException("processor failed before returning a Uni");
    when(commandProcessor.processCommand(context, command)).thenThrow(failure);

    assertThatThrownBy(
            () ->
                mcpResource()
                    .processCommand(Uni.createFrom().item(context), command)
                    .await()
                    .atMost(Duration.ofSeconds(5)))
        .isSameAs(failure);
    verify(context).close();
  }

  @Test
  void closesContextWhenFeatureConfigurationIsInvalid() throws Exception {
    var context = commandContext(true);
    when(context.apiFeatures())
        .thenReturn(ApiFeatures.fromConfigAndRequest(Map.of(ApiFeature.MCP, "invalid"), null));

    assertThatThrownBy(
            () ->
                mcpResource()
                    .processCommand(Uni.createFrom().item(context), keyspaceCommand())
                    .await()
                    .atMost(Duration.ofSeconds(5)))
        .isInstanceOf(IllegalArgumentException.class);
    verifyNoInteractions(commandProcessor);
    verify(context).close();
  }

  @Test
  void closesContextWhenFeatureDisabled() throws Exception {
    var context = commandContext(false);
    var command = keyspaceCommand();

    var response =
        mcpResource()
            .processCommand(Uni.createFrom().item(context), command)
            .await()
            .atMost(Duration.ofSeconds(5));

    assertThat(response.isError()).isTrue();
    verifyNoInteractions(commandProcessor);
    verify(context, times(1)).close();
  }

  @Test
  void closesContextOnCancellation() throws Exception {
    var context = commandContext(true);
    var command = keyspaceCommand();
    when(commandProcessor.processCommand(context, command))
        .thenReturn(Uni.createFrom().emitter(emitter -> {}));

    var cancellable =
        mcpResource()
            .processCommand(Uni.createFrom().item(context), command)
            .subscribe()
            .with(response -> {}, failure -> {});
    cancellable.cancel();

    verify(context, times(1)).close();
  }

  @Test
  void closeFailureDoesNotReplaceResult() throws Exception {
    var context = commandContext(true);
    doThrow(new Exception("close failed")).when(context).close();
    var command = keyspaceCommand();
    var result = CommandResult.statusOnlyBuilder(RequestTracing.NO_OP).build();
    when(commandProcessor.processCommand(context, command))
        .thenReturn(Uni.createFrom().item(result));

    var response =
        mcpResource()
            .processCommand(Uni.createFrom().item(context), command)
            .await()
            .atMost(Duration.ofSeconds(5));

    assertThat(response.isError()).isFalse();
    verify(context, times(1)).close();
  }

  @Test
  void closeFailureDoesNotReplaceError() throws Exception {
    var context = commandContext(true);
    doThrow(new Exception("close failed")).when(context).close();
    var command = keyspaceCommand();
    when(commandProcessor.processCommand(context, command))
        .thenReturn(Uni.createFrom().failure(new RuntimeException("processor failed")));

    assertThatThrownBy(
            () ->
                mcpResource()
                    .processCommand(Uni.createFrom().item(context), command)
                    .await()
                    .atMost(Duration.ofSeconds(5)))
        .hasMessage("processor failed");
    verify(context, times(1)).close();
  }

  @Test
  void keyspaceContextUsesOneRequestContext() {
    var requestContextCaptor = ArgumentCaptor.forClass(RequestContext.class);
    when(schemaObjectCache.getKeyspace(requestContextCaptor.capture(), any(), any(), eq(true)))
        .thenReturn(Uni.createFrom().item(TEST_CONSTANTS.KEYSPACE_SCHEMA_OBJECT));

    var context =
        mcpResourceWithRequest()
            .buildKeyspaceContext(TEST_CONSTANTS.KEYSPACE_NAME, keyspaceCommand())
            .await()
            .atMost(Duration.ofSeconds(5));

    // The schema lookup and the built command context must see the same request, so the
    // call has one request ID from start to finish.
    assertThat(context.requestContext()).isSameAs(requestContextCaptor.getValue());
    assertThat(context.requestContext().requestId())
        .isEqualTo(requestContextCaptor.getValue().requestId());
  }

  @Test
  void serializationFailureIsLoggedAsTheFinalToolOutcome() throws Exception {
    var context = commandContext(true);
    var command = keyspaceCommand();
    when(commandProcessor.processCommand(context, command))
        .thenReturn(Uni.createFrom().item(CommandResult.statusOnlyBuilder(RequestTracing.NO_OP).build()));
    doThrow(new JsonProcessingException("synthetic-serialization-secret") {})
        .when(objectMapper).writeValueAsString(any());

    try (var logs = new LoggerTestWrapper(McpRequestLogging.class)) {
      var response = mcpResource().processCommand(Uni.createFrom().item(context), command)
          .await().atMost(Duration.ofSeconds(5));
      assertThat(response.isError()).isTrue();
      assertThat(logs.logRecords()).singleElement().satisfies(record -> {
        assertThat(record.getMessage()).contains("MCP command completed");
        assertThat(record.getParameters()).contains(true).doesNotContain("synthetic-serialization-secret");
        assertThat(record.getThrown()).isNull();
      });
    }
    verify(context).close();
  }

  private KeyspaceCommand keyspaceCommand() {
    var command = mock(KeyspaceCommand.class);
    when(command.commandName()).thenReturn(CommandName.FIND_COLLECTIONS);
    return command;
  }

  @SuppressWarnings("unchecked")
  private CommandContext<CollectionSchemaObject> commandContext(boolean mcpEnabled) {
    CommandContext<CollectionSchemaObject> context = mock(CommandContext.class);
    var apiFeatures =
        mcpEnabled
            ? ApiFeatures.fromConfigAndRequest(Map.of(ApiFeature.MCP, "true"), null)
            : ApiFeatures.empty();
    when(context.apiFeatures()).thenReturn(apiFeatures);
    when(context.requestTracing()).thenReturn(RequestTracing.NO_OP);
    return context;
  }

  private McpResource mcpResource() {
    return mcpResource(
        () -> mock(RoutingContext.class),
        () -> mock(SecurityIdentity.class),
        instanceOf(mock(RequestTenantResolver.class)),
        instanceOf(mock(EmbeddingCredentialsResolver.class)));
  }

  private McpResource mcpResource(
      Provider<RoutingContext> routingContextProvider,
      Provider<SecurityIdentity> securityIdentityProvider,
      Instance<RequestTenantResolver> tenantResolver,
      Instance<EmbeddingCredentialsResolver> embeddingCredentialsResolver) {
    return new McpResource(
        objectMapper,
        commandProcessor,
        mock(MeterRegistry.class),
        mock(JsonProcessingMetricsReporter.class),
        sessionCacheSupplier(),
        mock(EmbeddingProviderFactory.class),
        mock(RerankingProviderFactory.class),
        schemaCacheSupplier(),
        routingContextProvider,
        securityIdentityProvider,
        tenantResolver,
        embeddingCredentialsResolver);
  }

  /** An {@link McpResource} whose request can build a real {@link RequestContext}. */
  private McpResource mcpResourceWithRequest() {
    var httpRequest = mock(HttpServerRequest.class);
    when(httpRequest.headers()).thenReturn(mock(MultiMap.class));
    var routingContext = mock(RoutingContext.class);
    when(routingContext.request()).thenReturn(httpRequest);

    var principal = mock(Principal.class);
    when(principal.getName()).thenReturn("test-token");
    var securityIdentity = mock(SecurityIdentity.class);
    when(securityIdentity.getPrincipal()).thenReturn(principal);

    var tenantResolver = mock(RequestTenantResolver.class);
    when(tenantResolver.resolve(any(), any())).thenReturn(TEST_CONSTANTS.TENANT);
    var embeddingCredentialsResolver = mock(EmbeddingCredentialsResolver.class);
    when(embeddingCredentialsResolver.resolveEmbeddingCredentials(any(), any()))
        .thenReturn(mock(EmbeddingCredentials.class));

    return mcpResource(
        () -> routingContext,
        () -> securityIdentity,
        instanceOf(tenantResolver),
        instanceOf(embeddingCredentialsResolver));
  }

  private CqlSessionCacheSupplier sessionCacheSupplier() {
    var supplier = mock(CqlSessionCacheSupplier.class);
    when(supplier.get()).thenReturn(mock(CQLSessionCache.class));
    return supplier;
  }

  private SchemaObjectCacheSupplier schemaCacheSupplier() {
    var supplier = mock(SchemaObjectCacheSupplier.class);
    when(supplier.get()).thenReturn(schemaObjectCache);
    return supplier;
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> instanceOf(T value) {
    Instance<T> instance = mock(Instance.class);
    when(instance.get()).thenReturn(value);
    return instance;
  }
}
