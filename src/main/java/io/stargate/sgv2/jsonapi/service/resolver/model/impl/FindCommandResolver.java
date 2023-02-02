package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link FindOneCommand } */
@ApplicationScoped
public class FindCommandResolver extends FilterableResolver<FindCommand>
    implements CommandResolver<FindCommand> {

  private final DocumentConfig documentConfig;

  @Inject
  public FindCommandResolver(DocumentConfig documentConfig, ObjectMapper objectMapper) {
    super(objectMapper, false, true);
    this.documentConfig = documentConfig;
  }

  public FindCommandResolver() {
    this(null, null);
  }

  @Override
  public Class<FindCommand> getCommandClass() {
    return FindCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, FindCommand command) {
    return resolve(ctx, command);
  }

  @Override
  protected FilteringOptions getFilteringOption(FindCommand command) {
    int limit =
        command.options() != null && command.options().limit() != null
            ? command.options().limit()
            : documentConfig.maxLimit();
    int pageSize =
        command.options() != null && command.options().pageSize() != null
            ? command.options().pageSize()
            : documentConfig.defaultPageSize();
    String pagingState = command.options() != null ? command.options().pagingState() : null;
    return new FilteringOptions(limit, pagingState, pageSize);
  }
}
