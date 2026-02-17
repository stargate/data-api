package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.ConfigPreLoader;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentialsResolver;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.tenant.RequestTenantResolver;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Provider;

/**
 * Shared helper for MCP tool providers. Provides command context building and command execution via
 * the existing {@link MeteredCommandProcessor} pipeline.
 */
@ApplicationScoped
public class McpToolsHelper {

  private final ObjectMapper objectMapper;
  private final MeteredCommandProcessor meteredCommandProcessor;
  private final CommandContext.BuilderSupplier contextBuilderSupplier;
  private final EmbeddingProviderFactory embeddingProviderFactory;
  private final SchemaCache schemaCache;

  // Per-request dependencies for creating RequestContext in MCP (non-JAX-RS) context
  private final Provider<RoutingContext> routingContextProvider;
  private final Provider<SecurityIdentity> securityIdentityProvider;
  private final Instance<RequestTenantResolver> tenantResolver;
  private final Instance<EmbeddingCredentialsResolver> embeddingCredentialsResolver;

  @Inject
  public McpToolsHelper(
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

  // ---- JSON parsing helpers ----

  /** Parse a JSON string to a JsonNode, or return null if the input is null or blank. */
  public JsonNode parseJsonNode(String json) {
    if (json == null || json.isBlank()) {
      return null;
    }
    try {
      return objectMapper.readTree(json);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid JSON: " + e.getMessage(), e);
    }
  }

  /** Parse a JSON string to a FilterDefinition, or return null if input is null or blank. */
  public FilterDefinition toFilterDefinition(String filter) {
    JsonNode node = parseJsonNode(filter);
    return node != null ? new FilterDefinition(node) : null;
  }

  /** Parse a JSON string to a SortDefinition, or return null if input is null or blank. */
  public SortDefinition toSortDefinition(String sort) {
    JsonNode node = parseJsonNode(sort);
    return node != null ? new SortDefinition(node) : null;
  }

  /** Parse a JSON string to an UpdateClause via Jackson deserialization. */
  public UpdateClause toUpdateClause(String update) {
    if (update == null || update.isBlank()) {
      return null;
    }
    try {
      return objectMapper.readValue(update, UpdateClause.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid update clause JSON: " + e.getMessage(), e);
    }
  }

  /**
   * Deserialize a JSON string into a command type via Jackson. Used for complex commands where
   * direct construction is impractical.
   */
  public <T> T deserializeCommand(String json, Class<T> commandType) {
    try {
      return objectMapper.readValue(json, commandType);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Invalid command JSON for " + commandType.getSimpleName() + ": " + e.getMessage(), e);
    }
  }

  /**
   * Return an empty ObjectNode (equivalent to an empty JSON object {}) for use as a default
   * projection.
   */
  public JsonNode emptyObjectNode() {
    return JsonNodeFactory.instance.objectNode();
  }

  // ---- Command context building ----

  /** Build a CommandContext for database-level (general) commands. */
  public CommandContext<?> buildGeneralContext(GeneralCommand command) {
    RequestContext requestContext = createRequestContext();
    return contextBuilderSupplier
        .getBuilder(new DatabaseSchemaObject())
        .withCommandName(command.getClass().getSimpleName())
        .withRequestContext(requestContext)
        .withApiFeatures(ApiFeatures.empty())
        .build();
  }

  /** Build a CommandContext for keyspace-level commands. */
  public CommandContext<?> buildKeyspaceContext(String keyspace, String commandName) {
    RequestContext requestContext = createRequestContext();
    return contextBuilderSupplier
        .getBuilder(new KeyspaceSchemaObject(keyspace))
        .withEmbeddingProvider(null)
        .withCommandName(commandName)
        .withRequestContext(requestContext)
        .withApiFeatures(ApiFeatures.empty())
        .build();
  }

  /**
   * Build a CommandContext for collection-level or table-level commands. This requires looking up
   * the schema from the cache, similar to {@link
   * io.stargate.sgv2.jsonapi.api.v1.CollectionResource}.
   */
  public Uni<CommandContext<?>> buildCollectionContext(
      String keyspace, String collection, Command command) {
    RequestContext requestContext = createRequestContext();
    boolean isDdl = CommandType.DDL.equals(command.commandName().getCommandType());

    return schemaCache
        .getSchemaObject(requestContext, keyspace, collection, isDdl)
        .map(
            schemaObject -> {
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

              return contextBuilderSupplier
                  .getBuilder(schemaObject)
                  .withEmbeddingProvider(embeddingProvider)
                  .withCommandName(command.getClass().getSimpleName())
                  .withRequestContext(requestContext)
                  .withApiFeatures(ApiFeatures.empty())
                  .build();
            });
  }

  // ---- Command execution ----

  /**
   * Process a general or keyspace command and return the JSON-serialized result string.
   *
   * @param context The command context (from buildGeneralContext or buildKeyspaceContext)
   * @param command The command to execute
   * @return Uni of JSON result string
   */
  public Uni<String> processCommand(CommandContext<?> context, Command command) {
    return meteredCommandProcessor.processCommand(context, command).map(this::serializeResult);
  }

  /**
   * Process a collection/table command. Builds the context asynchronously and then executes.
   *
   * @param keyspace Keyspace name
   * @param collection Collection or table name
   * @param command The command to execute
   * @return Uni of JSON result string
   */
  public Uni<String> processCollectionCommand(String keyspace, String collection, Command command) {
    return buildCollectionContext(keyspace, collection, command)
        .flatMap(context -> meteredCommandProcessor.processCommand(context, command))
        .map(result -> serializeResult(result));
  }

  private String serializeResult(CommandResult result) {
    try {
      return objectMapper.writeValueAsString(result);
    } catch (JsonProcessingException e) {
      return "{\"errors\":[{\"message\":\"Failed to serialize result: " + e.getMessage() + "\"}]}";
    }
  }
}
