package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkiverse.mcp.server.ToolResponse;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.KeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentialsResolver;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.tenant.RequestTenantResolver;
import io.stargate.sgv2.jsonapi.api.request.token.PrincipalTokenResolver;
import io.stargate.sgv2.jsonapi.api.security.McpOAuthMetadata;
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
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.inject.Instance;
import jakarta.validation.Validator;
import java.security.Principal;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests for the command context of an MCP tool call. Each call gets one context, and that context
 * is closed once however the call ends.
 */
class McpResourceTest {

  private static final TestConstants TEST_CONSTANTS = new TestConstants();
  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final CommandResult OK =
      CommandResult.statusOnlyBuilder(RequestTracing.NO_OP).build();

  private final MeteredCommandProcessor commandProcessor = mock(MeteredCommandProcessor.class);
  private final SchemaObjectCache schemaObjectCache = mock(SchemaObjectCache.class);
  private final KeyspaceCommand command = keyspaceCommand();
  private final CommandContext<CollectionSchemaObject> context = commandContext();

  @Test
  void closesContextOnSuccess() throws Exception {
    when(commandProcessor.processCommand(context, command)).thenReturn(Uni.createFrom().item(OK));

    assertThat(callTool().isError()).isFalse();
    verify(context).close();
  }

  @Test
  void closesContextOnFailure() throws Exception {
    when(commandProcessor.processCommand(context, command))
        .thenReturn(Uni.createFrom().failure(new RuntimeException("processor failed")));

    assertThatThrownBy(this::callTool).hasMessage("processor failed");
    verify(context).close();
  }

  @Test
  void closesContextWhenProcessorThrows() throws Exception {
    // The processor throws before it returns a Uni.
    var failure = new IllegalStateException("processor failed");
    when(commandProcessor.processCommand(context, command)).thenThrow(failure);

    assertThatThrownBy(this::callTool).isSameAs(failure);
    verify(context).close();
  }

  @Test
  void closesContextWhenFeatureCheckThrows() throws Exception {
    // An unparseable flag value makes the feature check itself throw.
    when(context.apiFeatures())
        .thenReturn(ApiFeatures.fromConfigAndRequest(Map.of(ApiFeature.MCP, "invalid"), null));

    assertThatThrownBy(this::callTool).isInstanceOf(IllegalArgumentException.class);
    verifyNoInteractions(commandProcessor);
    verify(context).close();
  }

  @Test
  void closesContextWhenFeatureDisabled() throws Exception {
    when(context.apiFeatures()).thenReturn(ApiFeatures.empty());

    assertThat(callTool().isError()).isTrue();
    verifyNoInteractions(commandProcessor);
    verify(context).close();
  }

  @Test
  void closesContextOnCancellation() throws Exception {
    // A Uni that never emits keeps the call open until the client cancels it.
    when(commandProcessor.processCommand(context, command)).thenReturn(Uni.createFrom().nothing());

    mcpResource()
        .processCommand(() -> Uni.createFrom().item(context), command)
        .subscribe()
        .with(response -> {}, failure -> {})
        .cancel();

    verify(context).close();
  }

  @Test
  void cancelingOneOfTwoSubscriptionsDoesNotCloseOrReplaceTheOther() throws Exception {
    var canceledContext = commandContext();
    var survivingContext = commandContext();
    var canceledResult = new CompletableFuture<CommandResult>();
    var survivingResult = new CompletableFuture<CommandResult>();
    when(commandProcessor.processCommand(canceledContext, command))
        .thenReturn(Uni.createFrom().completionStage(canceledResult));
    when(commandProcessor.processCommand(survivingContext, command))
        .thenReturn(Uni.createFrom().completionStage(survivingResult));
    var resource = mcpResource();
    var canceledResponse = new CompletableFuture<ToolResponse>();
    var canceledSubscription =
        resource
            .processCommand(() -> Uni.createFrom().item(canceledContext), command)
            .subscribe()
            .with(canceledResponse::complete, canceledResponse::completeExceptionally);
    var survivingResponse =
        resource
            .processCommand(() -> Uni.createFrom().item(survivingContext), command)
            .subscribeAsCompletionStage();

    verify(commandProcessor).processCommand(canceledContext, command);
    verify(commandProcessor).processCommand(survivingContext, command);
    verify(canceledContext, never()).close();
    verify(survivingContext, never()).close();

    canceledSubscription.cancel();

    verify(canceledContext).close();
    verify(survivingContext, never()).close();
    assertThat(canceledResponse).isNotDone();
    assertThat(survivingResponse).isNotDone();

    survivingResult.complete(OK);

    assertThat(survivingResponse.get(5, TimeUnit.SECONDS).isError()).isFalse();
    assertThat(canceledResponse).isNotDone();
    verify(canceledContext).close();
    verify(survivingContext).close();
  }

  @Test
  void closeFailureDoesNotReplaceResult() throws Exception {
    doThrow(new Exception("close failed")).when(context).close();
    when(commandProcessor.processCommand(context, command)).thenReturn(Uni.createFrom().item(OK));

    assertThat(callTool().isError()).isFalse();
    verify(context).close();
  }

  @Test
  void closeFailureDoesNotReplaceError() throws Exception {
    doThrow(new Exception("close failed")).when(context).close();
    when(commandProcessor.processCommand(context, command))
        .thenReturn(Uni.createFrom().failure(new RuntimeException("processor failed")));

    assertThatThrownBy(this::callTool).hasMessage("processor failed");
    verify(context).close();
  }

  @Test
  void keyspaceContextUsesOneRequestContext() {
    // The schema lookup and the command context share one request, so the call keeps one
    // request ID from start to finish.
    var lookupRequest = ArgumentCaptor.forClass(RequestContext.class);
    when(schemaObjectCache.getKeyspace(lookupRequest.capture(), any(), any(), eq(true)))
        .thenReturn(Uni.createFrom().item(TEST_CONSTANTS.KEYSPACE_SCHEMA_OBJECT));

    var keyspaceContext =
        mcpResource()
            .buildKeyspaceContext(TEST_CONSTANTS.KEYSPACE_NAME, command)
            .await()
            .atMost(TIMEOUT);

    assertThat(keyspaceContext.requestContext()).isSameAs(lookupRequest.getValue());
  }

  /** Runs one tool call with {@link #context} and {@link #command} and waits for the response. */
  private ToolResponse callTool() {
    return mcpResource()
        .processCommand(() -> Uni.createFrom().item(context), command)
        .await()
        .atMost(TIMEOUT);
  }

  private static KeyspaceCommand keyspaceCommand() {
    var command = mock(KeyspaceCommand.class);
    when(command.commandName()).thenReturn(CommandName.FIND_COLLECTIONS);
    return command;
  }

  @SuppressWarnings("unchecked")
  private static CommandContext<CollectionSchemaObject> commandContext() {
    CommandContext<CollectionSchemaObject> context = mock(CommandContext.class);
    when(context.apiFeatures())
        .thenReturn(ApiFeatures.fromConfigAndRequest(Map.of(ApiFeature.MCP, "true"), null));
    when(context.requestTracing()).thenReturn(RequestTracing.NO_OP);
    return context;
  }

  /** An {@link McpResource} with enough of an HTTP request to build a real request context. */
  private McpResource mcpResource() {
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

    var sessionCacheSupplier = mock(CqlSessionCacheSupplier.class);
    when(sessionCacheSupplier.get()).thenReturn(mock(CQLSessionCache.class));
    var schemaCacheSupplier = mock(SchemaObjectCacheSupplier.class);
    when(schemaCacheSupplier.get()).thenReturn(schemaObjectCache);

    return new McpResource(
        mock(ObjectMapper.class),
        mock(Validator.class),
        mock(McpOAuthMetadata.class),
        commandProcessor,
        mock(MeterRegistry.class),
        mock(JsonProcessingMetricsReporter.class),
        sessionCacheSupplier,
        mock(EmbeddingProviderFactory.class),
        mock(RerankingProviderFactory.class),
        schemaCacheSupplier,
        () -> routingContext,
        () -> securityIdentity,
        instanceOf(tenantResolver),
        instanceOf(new PrincipalTokenResolver()),
        instanceOf(mock(EmbeddingCredentialsResolver.class)));
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> instanceOf(T value) {
    Instance<T> instance = mock(Instance.class);
    when(instance.get()).thenReturn(value);
    return instance;
  }
}
