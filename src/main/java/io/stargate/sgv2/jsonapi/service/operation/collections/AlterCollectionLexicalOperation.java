package io.stargate.sgv2.jsonapi.service.operation.collections;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.resolver.CreateCollectionCommandResolver;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionTableComment;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Operation that enables the lexical feature on an existing collection by adding the {@code
 * query_lexical_value} column, creating an analyzed SAI index on it, and updating the table
 * "comment" JSON to record the new lexical config.
 *
 * <p>When {@link #noOp} is true the operation returns success without executing any DDL: this is
 * used for the "already enabled with same settings" case.
 *
 * <p><b>No rollback on partial failure.</b> If e.g. ADD COLUMN succeeds but CREATE INDEX fails, the
 * column is left in place and the failure is propagated to the caller. This matches {@link
 * CreateCollectionOperation}'s behavior and is intentional:
 *
 * <ul>
 *   <li>Reverse DDL (DROP COLUMN, DROP INDEX) is itself fallible — a rollback that fails leaves the
 *       schema in a worse state than the original partial failure and obscures the root cause.
 *   <li>The operation is retry-safe: existence is checked against freshly-fetched metadata, so ADD
 *       COLUMN is skipped when the column already exists, CREATE INDEX uses {@code IF NOT EXISTS},
 *       and the comment write is a plain overwrite. Re-issuing the same {@code alterCollection}
 *       command after the underlying issue is resolved completes the unfinished steps without
 *       failing on the finished ones. (The backend does not support {@code ADD IF NOT EXISTS}, so
 *       the skip relies on the metadata check.)
 *   <li>Users get a consistent mental model with {@code createCollection}, which has the same
 *       partial-failure semantics.
 * </ul>
 *
 * <p>The comment is updated last, so an interrupted run can leave the column/index present while
 * {@code findCollections} still reports lexical as disabled; a successful retry reconciles this
 * (see {@code trulyEnabled} in {@code AlterCollectionCommandResolver}).
 */
public record AlterCollectionLexicalOperation(
    CommandContext<CollectionSchemaObject> commandContext,
    ObjectMapper objectMapper,
    DatabaseLimitsConfig dbLimitsConfig,
    int ddlDelayMillis,
    CollectionLexicalConfig newLexicalConfig,
    boolean noOp)
    implements Operation<CollectionSchemaObject> {

  private static final CqlIdentifier LEXICAL_COLUMN =
      CqlIdentifier.fromInternal(DocumentConstants.Columns.LEXICAL_INDEX_COLUMN_NAME);

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext requestContext, QueryExecutor queryExecutor) {

    if (noOp) {
      return Uni.createFrom().<Supplier<CommandResult>>item(new SchemaChangeResult(true));
    }

    final CollectionSchemaObject schemaObject = commandContext.schemaObject();
    final String keyspace = schemaObject.tableMetadata().getKeyspace().asInternal();
    final String table = schemaObject.tableMetadata().getName().asInternal();

    final String newComment;
    try {
      newComment = buildUpdatedComment(schemaObject);
    } catch (JacksonException | RuntimeException e) {
      // Resolver guarantees a V1 comment; if reading/updating still fails, surface a clean error
      // rather than a raw JacksonException/IllegalStateException.
      return Uni.createFrom()
          .failure(
              DatabaseException.Code.CORRUPTED_COLLECTION_SCHEMA.get(
                  errVars(
                      schemaObject,
                      map ->
                          map.put(
                              "errorMessage",
                              "Unable to update collection 'comment' to enable lexical: "
                                  + e.getMessage()))));
    }

    // Base all existence decisions on freshly-fetched metadata rather than the resolve-time
    // snapshot, so a column/index left by an interrupted prior run (or a concurrent op) is seen
    // here. This is also where we pre-flight the DB-wide index limit, before running any DDL.
    return queryExecutor
        .getDriverMetadata(requestContext)
        .map(Metadata::getKeyspaces)
        .flatMap(
            allKeyspaces -> {
              final TableMetadata currentTable =
                  Optional.ofNullable(allKeyspaces.get(schemaObject.tableMetadata().getKeyspace()))
                      .flatMap(ks -> ks.getTable(schemaObject.tableMetadata().getName()))
                      .orElse(schemaObject.tableMetadata());

              final boolean columnExists = currentTable.getColumn(LEXICAL_COLUMN).isPresent();
              final boolean indexExists =
                  currentTable
                      .getIndexes()
                      .containsKey(
                          CqlIdentifier.fromInternal(
                              CreateCollectionOperation.lexicalIndexName(table)));

              // Only an absent index is net-new, so only then enforce the limit (mirrors
              // CreateCollectionOperation): going over fails with TOO_MANY_INDEXES_FOR_COLLECTION
              // before any DDL, not a generic error from a failed CREATE INDEX.
              if (!indexExists) {
                final int saisUsed =
                    allKeyspaces.values().stream()
                        .flatMap(ks -> ks.getTables().values().stream())
                        .mapToInt(t -> t.getIndexes().size())
                        .sum();
                // enableLexical adds exactly one SAI (the analyzed lexical index).
                if (saisUsed + 1 > dbLimitsConfig.indexesAvailablePerDatabase()) {
                  return Uni.createFrom()
                      .<Supplier<CommandResult>>failure(
                          SchemaException.Code.TOO_MANY_INDEXES_FOR_COLLECTION.get(
                              errVars(schemaObject, map -> map.put("indexesPerCollection", "1"))));
                }
              }

              return executeLexicalDdl(
                  requestContext, queryExecutor, keyspace, table, newComment, columnExists);
            });
  }

  /**
   * Runs the enable-lexical DDL: ADD COLUMN (skipped when it already exists), CREATE CUSTOM INDEX
   * IF NOT EXISTS, then ALTER TABLE WITH comment, spaced by {@link #ddlDelayMillis}. The {@code
   * columnAlreadyExists} flag is derived from freshly-fetched metadata so a leftover column is
   * skipped rather than failing the (plain) ADD — the backend does not support {@code ADD IF NOT
   * EXISTS}.
   */
  private Uni<Supplier<CommandResult>> executeLexicalDdl(
      RequestContext requestContext,
      QueryExecutor queryExecutor,
      String keyspace,
      String table,
      String newComment,
      boolean columnAlreadyExists) {

    SimpleStatement createIndexStmt =
        CreateCollectionOperation.buildLexicalIndexStatement(
            keyspace, table, newLexicalConfig, /* ifNotExists */ true);

    // Cassandra does not accept bind parameters for table options like `comment`, so the comment
    // JSON is embedded directly into the CQL (as createCollection does); single quotes are doubled
    // to keep the string literal valid.
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
              "ALTER TABLE \"%s\".\"%s\" ADD %s text"
                  .formatted(keyspace, table, DocumentConstants.Columns.LEXICAL_INDEX_COLUMN_NAME));
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
    final String comment = CollectionTableComment.rawComment(schemaObject.tableMetadata());
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
    CreateCollectionCommandResolver.addLexicalToOptionsNode(optionsNode, newLexicalConfig);
    return objectMapper.writeValueAsString(rootNode);
  }
}
