package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkiverse.mcp.server.MetaKey;
import io.quarkiverse.mcp.server.ToolResponse;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.ConfigPreLoader;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentialsResolver;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.tenant.RequestTenantResolver;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import java.util.Map;
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
  private final SchemaCache schemaCache;

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
      SchemaCache schemaCache,
      Provider<RoutingContext> routingContextProvider,
      Provider<SecurityIdentity> securityIdentityProvider,
      Instance<RequestTenantResolver> tenantResolver,
      Instance<EmbeddingCredentialsResolver> embeddingCredentialsResolver) {

    // TODO: these vars are needed to replicate what we do in GeneralResource,
    // KeyspaceResource, etc. We should refactor to avoid duplication later.
    this.objectMapper = objectMapper;
    this.meteredCommandProcessor = meteredCommandProcessor;
    this.embeddingProviderFactory = embeddingProviderFactory;
    this.schemaCache = schemaCache;

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
  public CommandContext<?> buildGeneralContext(GeneralCommand command) {
    return contextBuilderSupplier
        .getBuilder(new DatabaseSchemaObject())
        .withCommandName(command.getClass().getSimpleName())
        .withRequestContext(createRequestContext())
        .build();
  }

  /**
   * Build a CommandContext for database-level (general) commands. similar to {@link
   * io.stargate.sgv2.jsonapi.api.v1.KeyspaceResource}
   */
  public CommandContext<?> buildKeyspaceContext(String keyspace, KeyspaceCommand command) {
    return contextBuilderSupplier
        .getBuilder(new KeyspaceSchemaObject(keyspace))
        .withCommandName(command.getClass().getSimpleName())
        .withRequestContext(createRequestContext())
        .build();
  }

  /**
   * Process a collection-level command. This is fully asynchronous as it needs to resolve the
   * SchemaObject dynamically from the SchemaCache, configure Vector search context if applicable,
   * before delegating execution to processCommand.
   */
  public Uni<ToolResponse> processCollectionCommand(
      String keyspace, String collection, CollectionCommand command) {

    RequestContext requestContext = createRequestContext();

    return schemaCache
        .getSchemaObject(
            requestContext,
            keyspace,
            collection,
            CommandType.DDL.equals(command.commandName().getCommandType()))
        .onItemOrFailure()
        .transformToUni(
            (schemaObject, throwable) -> {
              if (throwable != null) {
                // If schema resolution or authorization fails, return an error ToolResponse
                CommandResult errorResult =
                    CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
                        .addThrowable(throwable)
                        .build();
                return Uni.createFrom()
                    .item(new ToolResponse(true, null, errorResult.errors(), Map.of()));
              } else {
                VectorColumnDefinition vectorColDef = null;
                if (schemaObject.type() == SchemaObject.SchemaObjectType.COLLECTION) {
                  vectorColDef =
                      schemaObject
                          .vectorConfig()
                          .getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD)
                          .orElse(null);
                } else if (schemaObject.type() == SchemaObject.SchemaObjectType.TABLE) {
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

                var commandContext =
                    contextBuilderSupplier
                        .getBuilder(schemaObject)
                        .withEmbeddingProvider(embeddingProvider)
                        .withCommandName(command.getClass().getSimpleName())
                        .withRequestContext(requestContext)
                        .build();

                return processCommand(commandContext, command);
              }
            });
  }

  /**
   * Process a general or keyspace command and return the CommandResult directly.
   *
   * <p>The Quarkus MCP Server framework automatically encodes the {@link CommandResult} to JSON via
   * its built-in encoder (Jackson), so no manual serialization is needed.
   *
   * @param context The command context (from buildGeneralContext or buildKeyspaceContext)
   * @param command The command to execute
   * @return Uni of CommandResult
   */
  public Uni<ToolResponse> processCommand(CommandContext<?> context, Command command) {

    if (!context.apiFeatures().isFeatureEnabled(ApiFeature.MCP)) {
      var exception = SchemaException.Code.MCP_FEATURE_NOT_ENABLED.get();
      CommandResult errorResult =
          CommandResult.statusOnlyBuilder(context.requestTracing()).addThrowable(exception).build();
      return Uni.createFrom().item(new ToolResponse(true, null, errorResult.errors(), Map.of()));
    }

    return meteredCommandProcessor
        .processCommand(context, command)
        .map(
            result -> {
              // Success: structuredContent = the whole result data payload
              if (result.errors() == null || result.errors().isEmpty()) {
                return ToolResponse.structuredSuccess(result);
              }

              // Error: isError=true, structuredContent=errors array, _meta = status (if present)
              Map<MetaKey, Object> meta = null;
              if (result.status() != null && !result.status().isEmpty()) {
                meta = Map.of(MetaKey.of("status"), result.status());
              }
              return new ToolResponse(true, null, result.errors(), meta);
            });
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
