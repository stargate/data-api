package io.stargate.sgv2.jsonapi.service.resolver;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterCollectionOperationImpl;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.AlterCollectionLexicalOperation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionTableComment;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Objects;

@ApplicationScoped
public class AlterCollectionCommandResolver implements CommandResolver<AlterCollectionCommand> {

  private static final CqlIdentifier LEXICAL_COLUMN =
      CqlIdentifier.fromInternal(DocumentConstants.Columns.LEXICAL_INDEX_COLUMN_NAME);

  private final ObjectMapper objectMapper;
  private final DatabaseLimitsConfig dbLimitsConfig;

  @Inject
  public AlterCollectionCommandResolver(
      ObjectMapper objectMapper, DatabaseLimitsConfig dbLimitsConfig) {
    this.objectMapper = objectMapper;
    this.dbLimitsConfig = dbLimitsConfig;
  }

  @Override
  public Class<AlterCollectionCommand> getCommandClass() {
    return AlterCollectionCommand.class;
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, AlterCollectionCommand command) {

    if (command.operation() == null) {
      throw badOptions("must specify 'operation' field");
    }

    // Sealed interface: switch is exhaustive, so a new operation subtype fails to compile until
    // handled here.
    return switch (command.operation()) {
      case AlterCollectionOperationImpl.EnableLexical op -> handleEnableLexical(ctx, op);
    };
  }

  private Operation<CollectionSchemaObject> handleEnableLexical(
      CommandContext<CollectionSchemaObject> ctx, AlterCollectionOperationImpl.EnableLexical op) {

    // Reject legacy / pre-lexical collections up front: must have a V1 comment with
    // collection.options. Doing this before analyzer validation gives users the actionable
    // "recreate the collection" error on legacy schemas instead of an analyzer-validation
    // error they can't act on.
    if (!CollectionTableComment.hasV1Options(objectMapper, ctx.schemaObject().tableMetadata())) {
      throw badOptions(
          "collection has legacy metadata (pre-lexical schema); recreate the collection with lexical enabled");
    }

    // Synthesize a LexicalDesc with enabled=true so we can reuse the existing
    // validation pipeline that createCollection uses.
    final var lexicalDesc =
        new CreateCollectionCommand.Options.LexicalDesc(
            /* enabled */ Boolean.TRUE, op.analyzerDef());

    // fromApiDesc throws:
    //   - LEXICAL_FEATURE_NOT_ENABLED via the SchemaFactory if the feature is disabled
    //   - INVALID_ALTER_COLLECTION_OPTIONS for malformed analyzer
    final CollectionLexicalDef requested =
        CollectionLexicalDef.fromApiDesc(
                objectMapper,
                lexicalDesc,
                ctx.versionedSchema().lexicalDef(),
                SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS)
            .runningValue();

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
          ctx, objectMapper, dbLimitsConfig, ddlDelayMillis, requested, /* noOp */ false);
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
        ctx, objectMapper, dbLimitsConfig, ddlDelayMillis, requested, /* noOp */ true);
  }

  private static SchemaException badOptions(String message) {
    return SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS.get(Map.of("message", message));
  }
}
