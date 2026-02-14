package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.ConfigPreLoader;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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
  private final RequestContext requestContext;

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
      RequestContext requestContext) {
    this.objectMapper = objectMapper;
    this.meteredCommandProcessor = meteredCommandProcessor;
    this.embeddingProviderFactory = embeddingProviderFactory;
    this.schemaCache = schemaCache;
    this.requestContext = requestContext;

    this.contextBuilderSupplier =
        CommandContext.builderSupplier()
            .withJsonProcessingMetricsReporter(jsonProcessingMetricsReporter)
            .withCqlSessionCache(sessionCacheSupplier.get())
            .withCommandConfig(ConfigPreLoader.getPreLoadOrEmpty())
            .withEmbeddingProviderFactory(embeddingProviderFactory)
            .withRerankingProviderFactory(rerankingProviderFactory)
            .withMeterRegistry(meterRegistry);
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
  public CommandContext<?> buildGeneralContext(String commandName) {
    return contextBuilderSupplier
        .getBuilder(new DatabaseSchemaObject())
        .withCommandName(commandName)
        .withRequestContext(requestContext)
        .build();
  }

  /** Build a CommandContext for keyspace-level commands. */
  public CommandContext<?> buildKeyspaceContext(String keyspace, String commandName) {
    return contextBuilderSupplier
        .getBuilder(new KeyspaceSchemaObject(keyspace))
        .withEmbeddingProvider(null)
        .withCommandName(commandName)
        .withRequestContext(requestContext)
        .build();
  }

  /**
   * Build a CommandContext for collection-level or table-level commands. This requires looking up
   * the schema from the cache, similar to {@link
   * io.stargate.sgv2.jsonapi.api.v1.CollectionResource}.
   */
  public Uni<CommandContext<?>> buildCollectionContext(
      String keyspace, String collection, Command command) {
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
    return meteredCommandProcessor
        .processCommand(context, command)
        .map(result -> serializeResult(result));
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
