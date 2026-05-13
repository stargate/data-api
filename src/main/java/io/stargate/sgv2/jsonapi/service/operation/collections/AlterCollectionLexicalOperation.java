package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * Operation that enables the lexical feature on an existing collection by adding the {@code
 * query_lexical_value} column, creating an analyzed SAI index on it, and updating the table
 * "comment" JSON to record the new lexical config.
 *
 * <p>When {@link #noOp} is true the operation returns success without executing any DDL: this is
 * the "already enabled with same settings" case.
 *
 * <p>On partial failure (e.g. column added but index creation failed) earlier steps are not rolled
 * back; the failure is propagated to the caller, matching {@link CreateCollectionOperation}'s
 * behavior. The command is safe to retry once the underlying issue is resolved.
 */
public record AlterCollectionLexicalOperation(
    CommandContext<CollectionSchemaObject> commandContext,
    ObjectMapper objectMapper,
    int ddlDelayMillis,
    CollectionLexicalConfig newLexicalConfig,
    boolean noOp)
    implements Operation<CollectionSchemaObject> {

  private static final CqlIdentifier COMMENT_OPTION = CqlIdentifier.fromInternal("comment");

  private static final CqlIdentifier LEXICAL_COLUMN =
      CqlIdentifier.fromInternal(DocumentConstants.Columns.LEXICAL_INDEX_COLUMN_NAME);

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext requestContext, QueryExecutor queryExecutor) {

    if (noOp) {
      // Type witness needed: Mutiny's item(T) and item(Supplier<? extends T>) overloads otherwise
      // both match SchemaChangeResult (which is a Supplier<CommandResult>), and inference picks
      // the wrong T.
      return Uni.createFrom().<Supplier<CommandResult>>item(new SchemaChangeResult(true));
    }

    final CollectionSchemaObject schemaObject = commandContext.schemaObject();
    final String keyspace = schemaObject.tableMetadata().getKeyspace().asInternal();
    final String table = schemaObject.tableMetadata().getName().asInternal();

    final String newComment;
    try {
      newComment = buildUpdatedComment(schemaObject);
    } catch (JacksonException e) {
      return Uni.createFrom().failure(e);
    }

    final JsonNode analyzerDef = newLexicalConfig.analyzerDefinition();
    final String analyzerString =
        analyzerDef.isTextual() ? analyzerDef.asText() : analyzerDef.toString();

    // Idempotent for retry after partial failure: skip ADD COLUMN if the column already exists.
    final boolean columnAlreadyExists =
        schemaObject.tableMetadata().getColumn(LEXICAL_COLUMN).isPresent();

    final String lexicalCol = DocumentConstants.Columns.LEXICAL_INDEX_COLUMN_NAME;
    SimpleStatement createIndexStmt =
        SimpleStatement.newInstance(
            ("CREATE CUSTOM INDEX IF NOT EXISTS \"%s_%s\""
                    + " ON \"%s\".\"%s\" (%s)"
                    + " USING 'StorageAttachedIndex'"
                    + " WITH OPTIONS = { 'index_analyzer': '%s' }")
                .formatted(table, lexicalCol, keyspace, table, lexicalCol, analyzerString));

    // Cassandra does not accept bind parameters for table options like `comment`; embed the
    // JSON directly with CQL single-quote escaping (matches
    // CreateCollectionOperation.getCreateTable).
    SimpleStatement alterCommentStmt =
        SimpleStatement.newInstance(
            "ALTER TABLE \"%s\".\"%s\" WITH comment = '%s'"
                .formatted(keyspace, table, newComment.replace("'", "''")));

    final Duration delay = Duration.ofMillis(ddlDelayMillis > 0 ? ddlDelayMillis : 100);

    Uni<AsyncResultSet> pipeline;
    if (columnAlreadyExists) {
      pipeline = queryExecutor.executeCreateSchemaChange(requestContext, createIndexStmt);
    } else {
      SimpleStatement addColumnStmt =
          SimpleStatement.newInstance(
              "ALTER TABLE \"%s\".\"%s\" ADD %s text".formatted(keyspace, table, lexicalCol));
      pipeline =
          queryExecutor
              .executeCreateSchemaChange(requestContext, addColumnStmt)
              .onItem()
              .delayIt()
              .by(delay)
              .onItem()
              .transformToUni(
                  r1 -> queryExecutor.executeCreateSchemaChange(requestContext, createIndexStmt));
    }

    return pipeline
        .onItem()
        .delayIt()
        .by(delay)
        .onItem()
        .transformToUni(
            r2 -> queryExecutor.executeCreateSchemaChange(requestContext, alterCommentStmt))
        .map(r3 -> new SchemaChangeResult(true));
  }

  /**
   * Reads the current table comment JSON and surgically replaces the {@code
   * collection.options.lexical} sub-node, leaving all other options (vector / indexing / id /
   * rerank / unknown fields) untouched.
   *
   * <p>The resolver guarantees we are operating on a V1-shaped comment (legacy/V0 collections are
   * rejected before reaching the operation).
   */
  private String buildUpdatedComment(CollectionSchemaObject schemaObject) throws JacksonException {
    final Object commentObj = schemaObject.tableMetadata().getOptions().get(COMMENT_OPTION);
    final String comment = commentObj == null ? null : commentObj.toString();
    if (comment == null || comment.isBlank()) {
      // Defensive: resolver should have rejected this case.
      throw new IllegalStateException(
          "Cannot alter collection: table comment is empty; expected V1 schema");
    }

    final ObjectNode rootNode = (ObjectNode) objectMapper.readTree(comment);
    final ObjectNode collectionNode =
        (ObjectNode) rootNode.get(TableCommentConstants.TOP_LEVEL_KEY);
    if (collectionNode == null) {
      // Defensive: resolver should have rejected this case.
      throw new IllegalStateException(
          "Cannot alter collection: comment does not have '"
              + TableCommentConstants.TOP_LEVEL_KEY
              + "' node");
    }
    ObjectNode optionsNode = (ObjectNode) collectionNode.get(TableCommentConstants.OPTIONS_KEY);
    if (optionsNode == null) {
      optionsNode = objectMapper.createObjectNode();
      collectionNode.set(TableCommentConstants.OPTIONS_KEY, optionsNode);
    }
    optionsNode.set(
        TableCommentConstants.COLLECTION_LEXICAL_CONFIG_KEY,
        objectMapper.valueToTree(newLexicalConfig));
    return objectMapper.writeValueAsString(rootNode);
  }
}
