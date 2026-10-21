package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterCollectionOperation;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.SchemaChangeResult;
import io.stargate.sgv2.jsonapi.service.schema.CollectionSchemaVersion;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionOpenSearchDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionOpenSearchDefSchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Resolves the {@link AlterCollectionCommand} — adds or drops an OpenSearch index on an existing
 * collection without recreating it.
 *
 * <p>For {@code addOpenSearch}: emits {@code CREATE CUSTOM INDEX … USING 'OpenSearchIndex'} then
 * updates the table comment to persist the new schema.
 *
 * <p>For {@code dropOpenSearch}: emits {@code DROP INDEX IF EXISTS …} then updates the table
 * comment to remove the OpenSearch block.
 */
@ApplicationScoped
public class AlterCollectionCommandResolver implements CommandResolver<AlterCollectionCommand> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** {@code CREATE CUSTOM INDEX IF NOT EXISTS "<sai>" ON "<ks>"."<coll>" (doc_json) USING ... } */
  private static final String CREATE_OPEN_SEARCH_INDEX_CQL =
      "CREATE CUSTOM INDEX IF NOT EXISTS \"%s\" ON \"%s\".\"%s\" (doc_json) "
          + "USING 'OpenSearchIndex' WITH OPTIONS = %s";

  private static final String DROP_INDEX_CQL = "DROP INDEX IF EXISTS \"%s\".\"%s\"";

  /** {@code ALTER TABLE "ks"."table" WITH comment='...'} — single-quotes in comment are doubled. */
  private static final String ALTER_COMMENT_CQL = "ALTER TABLE \"%s\".\"%s\" WITH comment = '%s'";

  @Override
  public Class<AlterCollectionCommand> getCommandClass() {
    return AlterCollectionCommand.class;
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> commandContext, AlterCollectionCommand command) {

    var schemaObject = commandContext.schemaObject();

    return switch (command.operation()) {
      case AlterCollectionOperation.AddOpenSearch aos ->
          buildAddOpenSearchOp(commandContext, schemaObject, aos);
      case AlterCollectionOperation.DropOpenSearch dos ->
          buildDropOpenSearchOp(commandContext, schemaObject);
      default ->
          throw new IllegalStateException(
              "Unexpected AlterCollectionOperation: "
                  + command.operation().getClass().getSimpleName());
    };
  }

  // ── addOpenSearch ─────────────────────────────────────────────────────────────

  private Operation<CollectionSchemaObject> buildAddOpenSearchOp(
      CommandContext<CollectionSchemaObject> ctx,
      CollectionSchemaObject schemaObject,
      AlterCollectionOperation.AddOpenSearch aos) {

    // 1. Guard: no duplicate OpenSearch index
    if (schemaObject.openSearchDef() != null && schemaObject.openSearchDef().enabled()) {
      throw SchemaException.Code.OPEN_SEARCH_INDEX_ALREADY_EXISTS.get(errVars(schemaObject));
    }

    // 2. Validate descriptor and build the CollectionOpenSearchDef
    var keyspace = schemaObject.identifier().keyspace().asInternal();
    var collectionName = schemaObject.identifier().table().asInternal();

    var openSearchHolder =
        CollectionOpenSearchDef.fromApiDesc(
            aos.toOpenSearchDesc(),
            keyspace,
            collectionName,
            new CollectionOpenSearchDefSchemaFactory());
    var openSearchDef = openSearchHolder.runningValue();

    // 3. Prepare CQL statements (built eagerly — no lazy evaluation needed)
    var createIndexCql =
        CREATE_OPEN_SEARCH_INDEX_CQL.formatted(
            openSearchDef.saiIndexName(),
            keyspace,
            collectionName,
            buildCqlOptionsLiteral(buildCreateIndexOptions(openSearchDef)));
    var updatedComment = buildComment(schemaObject, openSearchDef);
    var alterCommentCql =
        ALTER_COMMENT_CQL.formatted(keyspace, collectionName, updatedComment.replace("'", "''"));

    return new AddOpenSearchCollectionOperation(createIndexCql, alterCommentCql);
  }

  // ── dropOpenSearch ────────────────────────────────────────────────────────────

  private Operation<CollectionSchemaObject> buildDropOpenSearchOp(
      CommandContext<CollectionSchemaObject> ctx, CollectionSchemaObject schemaObject) {

    // 1. Guard: OpenSearch must already be enabled
    if (schemaObject.openSearchDef() == null || !schemaObject.openSearchDef().enabled()) {
      throw SchemaException.Code.OPEN_SEARCH_NOT_ENABLED_FOR_COLLECTION.get(errVars(schemaObject));
    }

    var keyspace = schemaObject.identifier().keyspace().asInternal();
    var collectionName = schemaObject.identifier().table().asInternal();

    // 2. SAI index name is the auto-generated hcd_<ks>_<coll>
    var dropIndexCql =
        DROP_INDEX_CQL.formatted(keyspace, schemaObject.openSearchDef().saiIndexName());
    var updatedComment = buildComment(schemaObject, null);
    var alterCommentCql =
        ALTER_COMMENT_CQL.formatted(keyspace, collectionName, updatedComment.replace("'", "''"));

    return new DropOpenSearchCollectionOperation(dropIndexCql, alterCommentCql);
  }

  // ── Operation implementations ─────────────────────────────────────────────────

  /** Reactive operation: issues CREATE CUSTOM INDEX then ALTER TABLE WITH comment. */
  private record AddOpenSearchCollectionOperation(String createIndexCql, String alterCommentCql)
      implements Operation<CollectionSchemaObject> {

    @Override
    public Uni<Supplier<CommandResult>> execute(
        RequestContext requestContext, QueryExecutor queryExecutor) {
      return queryExecutor
          .executeCreateSchemaChange(requestContext, SimpleStatement.newInstance(createIndexCql))
          .flatMap(
              ignored ->
                  queryExecutor
                      .executeCreateSchemaChange(
                          requestContext, SimpleStatement.newInstance(alterCommentCql))
                      .map(result -> new SchemaChangeResult(true)));
    }
  }

  /** Reactive operation: issues DROP INDEX IF EXISTS then ALTER TABLE WITH comment. */
  private record DropOpenSearchCollectionOperation(String dropIndexCql, String alterCommentCql)
      implements Operation<CollectionSchemaObject> {

    @Override
    public Uni<Supplier<CommandResult>> execute(
        RequestContext requestContext, QueryExecutor queryExecutor) {
      return queryExecutor
          .executeDropSchemaChange(requestContext, SimpleStatement.newInstance(dropIndexCql))
          .flatMap(
              ignored ->
                  queryExecutor
                      .executeCreateSchemaChange(
                          requestContext, SimpleStatement.newInstance(alterCommentCql))
                      .map(result -> new SchemaChangeResult(true)));
    }
  }

  // ── Comment builder ───────────────────────────────────────────────────────────

  /**
   * Reconstructs the full table-comment JSON from the live schema object, injecting {@code
   * newOpenSearchDef} (or omitting the OpenSearch block when {@code null}).
   */
  private static String buildComment(
      CollectionSchemaObject schema, CollectionOpenSearchDef newOpenSearchDef) {

    // Derive the options from the existing collection settings
    var existingCmd = CollectionSchemaObject.collectionSettingToCreateCollectionCommand(schema);
    var existingOptions = existingCmd.options();

    var optionsNode = OBJECT_MAPPER.createObjectNode();

    if (existingOptions != null) {
      if (existingOptions.indexing() != null) {
        optionsNode.putPOJO(
            TableCommentConstants.COLLECTION_INDEXING_KEY, existingOptions.indexing());
      }
      if (existingOptions.vector() != null) {
        optionsNode.putPOJO(TableCommentConstants.COLLECTION_VECTOR_KEY, existingOptions.vector());
      }
    }

    // defaultId — always required
    if (existingOptions != null && existingOptions.idConfig() != null) {
      optionsNode.putPOJO(TableCommentConstants.DEFAULT_ID_KEY, existingOptions.idConfig());
    } else {
      optionsNode.set(
          TableCommentConstants.DEFAULT_ID_KEY, OBJECT_MAPPER.createObjectNode().put("type", ""));
    }

    // Lexical config — use the live def so we don't lose its state
    optionsNode.putPOJO(TableCommentConstants.COLLECTION_LEXICAL_CONFIG_KEY, schema.lexicalDef());

    // Rerank config
    optionsNode.putPOJO(TableCommentConstants.COLLECTION_RERANKING_CONFIG_KEY, schema.rerankDef());

    // OpenSearch block: present when adding, absent when dropping
    if (newOpenSearchDef != null && newOpenSearchDef.enabled()) {
      optionsNode.putPOJO(
          TableCommentConstants.COLLECTION_OPEN_SEARCH_CONFIG_KEY, newOpenSearchDef);
    }

    ObjectNode collectionNode = OBJECT_MAPPER.createObjectNode();
    collectionNode.put(
        TableCommentConstants.COLLECTION_NAME_KEY, schema.identifier().table().asInternal());
    collectionNode.put(
        TableCommentConstants.SCHEMA_VERSION_KEY,
        CollectionSchemaVersion.CURRENT_VERSION.ordinalValue());
    collectionNode.set(TableCommentConstants.OPTIONS_KEY, optionsNode);

    ObjectNode tableCommentNode = OBJECT_MAPPER.createObjectNode();
    tableCommentNode.set(TableCommentConstants.TOP_LEVEL_KEY, collectionNode);
    return tableCommentNode.toString();
  }

  // ── WITH OPTIONS CQL helpers ──────────────────────────────────────────────────

  private static Map<String, String> buildCreateIndexOptions(CollectionOpenSearchDef def) {
    Map<String, String> opts = new LinkedHashMap<>();
    opts.put("indexName", def.indexName());
    opts.put("createIndexIfNotExists", "true");
    opts.put("unpackJsonFields", "doc_json");
    opts.put("applyDefaultSchema", "false");
    opts.put("applyCustomSchema", "true");
    opts.put("customMappingsJson", def.customMappings(OBJECT_MAPPER).toString());
    opts.put(
        "propertiesFromJsonFields",
        def.mappings().properties().stream()
            .map(Map.Entry::getKey)
            .reduce((a, b) -> a + "," + b)
            .orElse(""));
    if (def.numShards() != null) {
      opts.put("numShards", String.valueOf(def.numShards()));
    }
    if (def.numReplicas() != null) {
      opts.put("numReplicas", String.valueOf(def.numReplicas()));
    }
    return opts;
  }

  /** Serialises an options map to a CQL literal: {@code {'k1':'v1','k2':'v2'}}. */
  private static String buildCqlOptionsLiteral(Map<String, String> opts) {
    var sb = new StringBuilder("{");
    boolean first = true;
    for (var entry : opts.entrySet()) {
      if (!first) sb.append(",");
      first = false;
      sb.append("'").append(entry.getKey()).append("':'");
      sb.append(entry.getValue().replace("'", "\\'")).append("'");
    }
    sb.append("}");
    return sb.toString();
  }
}
