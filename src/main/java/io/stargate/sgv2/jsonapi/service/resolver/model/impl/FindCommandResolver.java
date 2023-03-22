package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import java.util.List;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link FindOneCommand } */
@ApplicationScoped
public class FindCommandResolver extends FilterableResolver<FindCommand>
    implements CommandResolver<FindCommand> {

  private final DocumentConfig documentConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public FindCommandResolver(DocumentConfig documentConfig, ObjectMapper objectMapper) {
    super();
    this.objectMapper = objectMapper;
    this.documentConfig = documentConfig;
  }

  @Override
  public Class<FindCommand> getCommandClass() {
    return FindCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, FindCommand command) {
    List<DBFilterBase> filters = resolve(commandContext, command);
    int limit =
        command.options() != null && command.options().limit() != null
            ? command.options().limit()
            : documentConfig.maxLimit();
    int pageSize = documentConfig.defaultPageSize();
    String pagingState = command.options() != null ? command.options().pagingState() : null;
    return new FindOperation(
        commandContext,
        filters,
        pagingState,
        limit,
        pageSize,
        ReadType.DOCUMENT,
        Optional.empty(),
        objectMapper);
  }
}
