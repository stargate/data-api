package io.stargate.sgv2.jsonapi.service.resolver;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterCollectionCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.AlterCollectionLexicalOperation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Objects;

@ApplicationScoped
public class AlterCollectionCommandResolver implements CommandResolver<AlterCollectionCommand> {

  private static final CqlIdentifier COMMENT_OPTION = CqlIdentifier.fromInternal("comment");

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
      throw SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS.get(
          Map.of("message", "must specify 'lexical' field"));
    }

    final boolean lexicalAvailableForDB = ctx.apiFeatures().isFeatureEnabled(ApiFeature.LEXICAL);

    // validateAndConstruct throws:
    //   - LEXICAL_NOT_AVAILABLE_FOR_DATABASE if requested.enabled && !lexicalAvailableForDB
    //   - INVALID_CREATE_COLLECTION_OPTIONS for malformed analyzer / missing 'enabled' / etc.
    final CollectionLexicalConfig requested =
        CollectionLexicalConfig.validateAndConstruct(
            objectMapper, lexicalAvailableForDB, command.lexical());

    // Phase 1: disabling lexical is not supported.
    if (!requested.enabled()) {
      throw SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS.get(
          Map.of(
              "message",
              "'lexical.enabled' must be true; alterCollection cannot disable lexical search"));
    }

    // Reject legacy / pre-lexical collections: must have a V1 comment with collection.options.
    final String rawComment = readTableComment(ctx.schemaObject());
    if (isLegacyComment(rawComment)) {
      throw SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS.get(
          Map.of(
              "message",
              "collection has legacy metadata (pre-lexical schema); recreate the collection to enable lexical"));
    }

    final CollectionLexicalConfig current = ctx.schemaObject().lexicalConfig();
    final int ddlDelayMillis =
        ctx.config().get(OperationsConfig.class).databaseConfig().ddlDelayMillis();

    if (current.enabled()) {
      if (analyzersEqual(current.analyzerDefinition(), requested.analyzerDefinition())) {
        // Same settings already in effect: no-op success.
        return new AlterCollectionLexicalOperation(
            ctx, objectMapper, ddlDelayMillis, requested, /* noOp */ true);
      }
      throw SchemaException.Code.INVALID_ALTER_COLLECTION_OPTIONS.get(
          Map.of(
              "message",
              "lexical is already enabled for this collection with a different analyzer configuration"));
    }

    return new AlterCollectionLexicalOperation(
        ctx, objectMapper, ddlDelayMillis, requested, /* noOp */ false);
  }

  private static String readTableComment(CollectionSchemaObject schemaObject) {
    final Object commentObj = schemaObject.tableMetadata().getOptions().get(COMMENT_OPTION);
    return commentObj == null ? null : commentObj.toString();
  }

  private boolean isLegacyComment(String rawComment) {
    if (rawComment == null || rawComment.isBlank()) {
      return true;
    }
    try {
      JsonNode root = objectMapper.readTree(rawComment);
      JsonNode optionsNode =
          root.path(TableCommentConstants.TOP_LEVEL_KEY).path(TableCommentConstants.OPTIONS_KEY);
      return optionsNode.isMissingNode() || !optionsNode.isObject();
    } catch (Exception e) {
      return true;
    }
  }

  private static boolean analyzersEqual(JsonNode a, JsonNode b) {
    if (a == null || b == null) {
      return a == b;
    }
    return Objects.equals(a, b);
  }
}
