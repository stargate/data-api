package io.stargate.sgv2.jsonapi.service.resolver;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterCollectionCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.AlterCollectionLexicalOperation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Objects;

@ApplicationScoped
public class AlterCollectionCommandResolver implements CommandResolver<AlterCollectionCommand> {

  private static final CqlIdentifier COMMENT_OPTION = CqlIdentifier.fromInternal("comment");

  private static final CqlIdentifier LEXICAL_COLUMN =
      CqlIdentifier.fromInternal(DocumentConstants.Columns.LEXICAL_INDEX_COLUMN_NAME);

  private final ObjectMapper objectMapper;

  @Inject
  public AlterCollectionCommandResolver(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public Class<AlterCollectionCommand> getCommandClass() {
    return AlterCollectionCommand.class;
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, AlterCollectionCommand command) {

    if (command.lexical() == null) {
      throw badOptions("must specify 'lexical' field");
    }

    // fromApiDesc throws:
    //   - LEXICAL_FEATURE_NOT_ENABLED if requested.enabled && feature is disabled (via
    // SchemaFactory)
    //   - INVALID_ALTER_COLLECTION_OPTIONS for malformed analyzer / missing 'enabled' / etc.
    final CollectionLexicalDef requested =
        CollectionLexicalDef.fromApiDesc(
                objectMapper,
                command.lexical(),
                ctx.versionedSchema().lexicalDef(),
                SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS)
            .runningValue();

    // Phase 1: disabling lexical is not supported.
    if (!requested.enabled()) {
      throw badOptions(
          "'lexical.enabled' must be true; alterCollection cannot disable lexical search");
    }

    // Reject legacy / pre-lexical collections: must have a V1 comment with collection.options.
    if (isLegacyComment(ctx.schemaObject())) {
      throw badOptions(
          "collection has legacy metadata (pre-lexical schema); recreate the collection with lexical enabled");
    }

    final CollectionLexicalDef current = ctx.schemaObject().lexicalDef();
    final int ddlDelayMillis =
        ctx.config().get(OperationsConfig.class).databaseConfig().ddlDelayMillis();

    // "Truly enabled" means both the stored comment claims lexical is on AND the underlying
    // column actually exists. If the comment says enabled but the column is missing (an
    // inconsistent state from manual surgery or an interrupted prior alter), treat it as
    // not-enabled and run the full DDL pipeline so the table catches up to the comment.
    final boolean trulyEnabled =
        current.enabled()
            && ctx.schemaObject().tableMetadata().getColumn(LEXICAL_COLUMN).isPresent();

    if (!trulyEnabled) {
      return new AlterCollectionLexicalOperation(
          ctx, objectMapper, ddlDelayMillis, requested, /* noOp */ false);
    }

    // Both analyzer definitions are guaranteed non-null here (CollectionLexicalDef's
    // constructor requires non-null analyzer when enabled=true). JsonNode.equals is value-based,
    // so this gives strict structural comparison for both string and object analyzers.
    if (!Objects.equals(current.analyzerDefinition(), requested.analyzerDefinition())) {
      throw badOptions(
          "lexical is already enabled for this collection with a different analyzer configuration");
    }
    // Same settings already in effect: no-op success.
    return new AlterCollectionLexicalOperation(
        ctx, objectMapper, ddlDelayMillis, requested, /* noOp */ true);
  }

  private static SchemaException badOptions(String message) {
    return SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS.get(Map.of("message", message));
  }

  private boolean isLegacyComment(CollectionSchemaObject schemaObject) {
    final Object commentObj = schemaObject.tableMetadata().getOptions().get(COMMENT_OPTION);
    if (commentObj == null) {
      return true;
    }
    try {
      JsonNode optionsNode =
          objectMapper
              .readTree(commentObj.toString())
              .path(TableCommentConstants.TOP_LEVEL_KEY)
              .path(TableCommentConstants.OPTIONS_KEY);
      return !optionsNode.isObject();
    } catch (Exception e) {
      return true;
    }
  }
}
