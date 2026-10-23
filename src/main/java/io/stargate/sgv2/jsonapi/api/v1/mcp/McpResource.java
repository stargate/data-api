package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkiverse.mcp.server.Cancellation;
import io.quarkiverse.mcp.server.InputRequiredException;
import io.quarkiverse.mcp.server.McpException;
import io.quarkiverse.mcp.server.ToolResponse;
import io.quarkus.security.AuthenticationException;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.ConfigPreLoader;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentialsResolver;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.tenant.RequestTenantResolver;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
import io.stargate.sgv2.jsonapi.service.schema.*;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.WebApplicationException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared resources for MCP tool providers. Provides command context building and command execution
 * via the existing {@link MeteredCommandProcessor} pipeline.
 */
@ApplicationScoped
public class McpResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(McpResource.class);

  private final ObjectMapper objectMapper;
  private final MeteredCommandProcessor meteredCommandProcessor;
  private final CommandContext.BuilderSupplier contextBuilderSupplier;
  private final EmbeddingProviderFactory embeddingProviderFactory;
  private final SchemaObjectCacheSupplier schemaObjectCacheSupplier;
  // Per-request dependencies for creating RequestContext in MCP (non-JAX-RS)
  // context
  private final Provider<RoutingContext> routingContextProvider;
  private final Provider<SecurityIdentity> securityIdentityProvider;
  private final Instance<RequestTenantResolver> tenantResolver;
  private final Instance<EmbeddingCredentialsResolver> embeddingCredentialsResolver;

  @Inject
  public McpResource(
      ObjectMapper objectMapper,
      MeteredCommandProcessor meteredCommandProcessor,
      MeterRegistry meterRegistry,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      CqlSessionCacheSupplier sessionCacheSupplier,
      EmbeddingProviderFactory embeddingProviderFactory,
      RerankingProviderFactory rerankingProviderFactory,
      SchemaObjectCacheSupplier schemaObjectCacheSupplier,
      Provider<RoutingContext> routingContextProvider,
      Provider<SecurityIdentity> securityIdentityProvider,
      Instance<RequestTenantResolver> tenantResolver,
      Instance<EmbeddingCredentialsResolver> embeddingCredentialsResolver) {

    // TODO: these vars are needed to replicate what we do in GeneralResource,
    // KeyspaceResource, etc. We should refactor to avoid duplication later.
    this.objectMapper = objectMapper;
    this.meteredCommandProcessor = meteredCommandProcessor;
    this.embeddingProviderFactory = embeddingProviderFactory;
    this.schemaObjectCacheSupplier = schemaObjectCacheSupplier;

    this.routingContextProvider = routingContextProvider;
    this.securityIdentityProvider = securityIdentityProvider;
    this.tenantResolver = tenantResolver;
    this.embeddingCredentialsResolver = embeddingCredentialsResolver;

    this.contextBuilderSupplier =
        CommandContext.builderSupplier()
            .withJsonProcessingMetricsReporter(jsonProcessingMetricsReporter)
            .withCqlSessionCache(sessionCacheSupplier.get())
            .withCommandConfig(ConfigPreLoader.getPreLoadOrEmpty())
            .withEmbeddingProviderFactory(embeddingProviderFactory)
            .withRerankingProviderFactory(rerankingProviderFactory)
            .withMeterRegistry(meterRegistry);
  }

  /**
   * Build a CommandContext for database-level (general) commands. similar to {@link
   * io.stargate.sgv2.jsonapi.api.v1.GeneralResource}
   */
  public Uni<CommandContext<?>> buildDatabaseContext(GeneralCommand command) {
    return Uni.createFrom()
        .deferred(
            () -> {
              var requestContext = createRequestContext();
              var dbIdentifier = SchemaObjectIdentifier.forDatabase(requestContext.tenant());

              return schemaObjectCacheSupplier
                  .get()
                  .getDatabase(requestContext, dbIdentifier, requestContext.userAgent())
                  .map(
                      databaseSchemaObject ->
                          contextBuilderSupplier
                              .getBuilder(databaseSchemaObject)
                              .withCommandName(command.getClass().getSimpleName())
                              .withRequestContext(requestContext)
                              .build());
            });
  }

  /**
   * Build a CommandContext for keyspace-level (keyspace) commands. similar to {@link
   * io.stargate.sgv2.jsonapi.api.v1.KeyspaceResource}
   */
  public Uni<CommandContext<?>> buildKeyspaceContext(String keyspace, KeyspaceCommand command) {
    return Uni.createFrom()
        .deferred(
            () -> {
              var requestContext = createRequestContext();
              var keyspaceIdentifier =
                  SchemaObjectIdentifier.forKeyspace(
                      requestContext.tenant(), cqlIdentifierFromUserInput(keyspace));

              // Force refresh on all keyspace commands because they are all DDL commands
              return schemaObjectCacheSupplier
                  .get()
                  .getKeyspace(requestContext, keyspaceIdentifier, requestContext.userAgent(), true)
                  .map(
                      keyspaceSchemaObject ->
                          contextBuilderSupplier
                              .getBuilder(keyspaceSchemaObject)
                              .withCommandName(command.getClass().getSimpleName())
                              .withRequestContext(requestContext)
                              .build());
            });
  }

  /**
   * Process a collection-level command. This is fully asynchronous as it needs to resolve the
   * SchemaObject dynamically from the SchemaCache, configure Vector search context if applicable,
   * before delegating execution to processCommand.
   */
  public Uni<ToolResponse> processCollectionCommand(
      String keyspace, String collection, CollectionCommand command) {
    return processCommand(
        () -> {
          var requestContext = createRequestContext();
          var unscopedSchemaIdentifier =
              new UnscopedSchemaObjectIdentifier.DefaultKeyspaceScopedName(
                  cqlIdentifierFromUserInput(keyspace), cqlIdentifierFromUserInput(collection));

          return schemaObjectCacheSupplier
              .get()
              .getTableBased(
                  requestContext,
                  unscopedSchemaIdentifier,
                  requestContext.userAgent(),
                  command.isForceSchemaRefresh())
              .map(
                  schemaObject -> {
                    VectorColumnDefinition vectorColDef = null;
                    if (schemaObject.type() == SchemaObjectType.COLLECTION) {
                      vectorColDef =
                          schemaObject
                              .vectorConfig()
                              .getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD)
                              .orElse(null);
                    } else if (schemaObject.type() == SchemaObjectType.TABLE) {
                      vectorColDef =
                          schemaObject
                              .vectorConfig()
                              .getFirstVectorColumnWithVectorizeDefinition()
                              .orElse(null);
                    }

                    EmbeddingProvider embeddingProvider = null;
                    if (vectorColDef != null && vectorColDef.vectorizeDefinition() != null) {
                      embeddingProvider =
                          embeddingProviderFactory.create(
                              requestContext.tenant(),
                              requestContext.authToken(),
                              vectorColDef.vectorizeDefinition().provider(),
                              vectorColDef.vectorizeDefinition().modelName(),
                              vectorColDef.vectorSize(),
                              vectorColDef.vectorizeDefinition().parameters(),
                              vectorColDef.vectorizeDefinition().authentication(),
                              command.getClass().getSimpleName());
                    }

                    return contextBuilderSupplier
                        .getBuilder(schemaObject)
                        .withEmbeddingProvider(embeddingProvider)
                        .withCommandName(command.getClass().getSimpleName())
                        .withRequestContext(requestContext)
                        .build();
                  });
        },
        command);
  }

  /**
   * Normalize context setup failures, then process a command and map its result to an MCP tool
   * response with readable text and structured content.
   *
   * @param contextSupplier Builds the command context after request logging is active
   * @param command The command to execute
   * @return Uni of ToolResponse
   */
  public Uni<ToolResponse> processCommand(
      Supplier<Uni<CommandContext<?>>> contextSupplier, Command command) {
    return Uni.createFrom()
        .deferred(
            () -> {
              Uni<CommandContext<?>> contextUni;
              try {
                // Mutiny captures context when callbacks are assembled.
                McpRequestLogging.activate(routingContextProvider.get());
                contextUni = contextSupplier.get();
              } catch (Exception failure) {
                contextUni = Uni.createFrom().failure(failure);
              }
              return contextUni
                  .onItemOrFailure()
                  .transformToUni(
                      (context, failure) ->
                          failure == null
                              ? executeCommand(context, command)
                              : handleSetupFailure(failure))
                  .map(result -> result.toToolResponse(objectMapper))
                  .invoke(result -> McpRequestLogging.commandCompleted(command, result))
                  .onFailure()
                  .invoke(failure -> McpRequestLogging.commandFailed(command));
            });
  }

  private Uni<CommandResult> executeCommand(CommandContext<?> context, Command command) {
    // Defer the feature check and processor call so synchronous failures still close the context.
    return Uni.createFrom()
        .deferred(
            () -> {
              if (!context.apiFeatures().isFeatureEnabled(ApiFeature.MCP)) {
                return Uni.createFrom()
                    .item(
                        CommandResult.statusOnlyBuilder(context.requestTracing())
                            .addThrowable(SchemaException.Code.MCP_FEATURE_NOT_ENABLED.get())
                            .build());
              }
              return meteredCommandProcessor.processCommand(context, command);
            })
        .onTermination()
        .invoke(
            () -> {
              try {
                context.close();
              } catch (Exception e) {
                LOGGER.error(
                    "Error closing the command context for requestContext={}",
                    context.requestContext(),
                    e);
              }
            });
  }

  private Uni<CommandResult> handleSetupFailure(Throwable failure) {
    // Synchronous cache reads use CompletableFuture.join(), unlike the asynchronous cache path.
    while (failure instanceof CompletionException && failure.getCause() != null) {
      failure = failure.getCause();
    }

    // Authentication, protocol errors, and framework control signals retain their own handling.
    if (failure instanceof Error
        || failure instanceof SecurityException
        || failure instanceof AuthenticationException
        || failure instanceof McpException
        || failure instanceof InputRequiredException
        || failure instanceof Cancellation.OperationCancellationException
        || failure instanceof CancellationException
        || (failure instanceof WebApplicationException webFailure
            && (webFailure.getResponse().getStatus() == 401
                || webFailure.getResponse().getStatus() == 403))) {
      return Uni.createFrom().failure(failure);
    }

    // Only APIException messages are intended for clients. Arbitrary setup failures may include
    // credentials or provider payloads, so do not pass them to the generic throwable mapper.
    if (!(failure instanceof APIException)) {
      McpRequestLogging.setupFailed();
    }
    var error =
        failure instanceof APIException apiException
            ? apiException
            : ServerException.Code.UNEXPECTED_SERVER_ERROR.withPreformattedMessage(
                "Unable to prepare the tool request. Retry the request or contact support.");
    return Uni.createFrom()
        .item(CommandResult.statusOnlyBuilder(RequestTracing.NO_OP).addThrowable(error).build());
  }

  /**
   * Create a {@link RequestContext} for the current MCP request. Uses the MCP-specific constructor
   * that resolves the auth token from {@link SecurityIdentity} instead of JAX-RS {@link
   * jakarta.ws.rs.core.SecurityContext}.
   */
  private RequestContext createRequestContext() {
    return new RequestContext(
        routingContextProvider.get(),
        securityIdentityProvider.get(),
        tenantResolver,
        embeddingCredentialsResolver);
  }
}
