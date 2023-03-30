package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndReplaceCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadAndUpdateOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link FindOneAndReplaceCommand } */
@ApplicationScoped
public class FindOneAndReplaceCommandResolver extends FilterableResolver<FindOneAndReplaceCommand>
    implements CommandResolver<FindOneAndReplaceCommand> {
  private final Shredder shredder;
  private final DocumentConfig documentConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public FindOneAndReplaceCommandResolver(
      ObjectMapper objectMapper, DocumentConfig documentConfig, Shredder shredder) {
    super();
    this.objectMapper = objectMapper;
    this.shredder = shredder;
    this.documentConfig = documentConfig;
  }

  @Override
  public Class<FindOneAndReplaceCommand> getCommandClass() {
    return FindOneAndReplaceCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, FindOneAndReplaceCommand command) {
    FindOperation findOperation = getFindOperation(commandContext, command);

    DocumentUpdater documentUpdater =
        DocumentUpdater.construct((ObjectNode) command.replacementDocument());

    // resolve options
    FindOneAndReplaceCommand.Options options = command.options();
    boolean returnUpdatedDocument =
        options != null && "after".equals(command.options().returnDocument());

    // return
    return new ReadAndUpdateOperation(
        commandContext,
        findOperation,
        documentUpdater,
        true,
        returnUpdatedDocument,
        false,
        shredder,
        command.buildProjector(),
        1,
        documentConfig.lwt().retries());
  }

  private FindOperation getFindOperation(
      CommandContext commandContext, FindOneAndReplaceCommand command) {
    List<DBFilterBase> filters = resolve(commandContext, command);
    SortClause sortClause = command.sortClause();
    if (sortClause != null && !sortClause.sortExpressions().isEmpty()) {
      // sort clause
    } else {

    }
    return new FindOperation(
        commandContext,
        filters,
        // 24-Mar-2023, tatu: Since we update the document, need to avoid modifications on
        // read path, hence pass identity projector.
        DocumentProjector.identityProjector(),
        null,
        1,
        1,
        ReadType.DOCUMENT,
        objectMapper,
        null,
        0,
        0);
  }
}
