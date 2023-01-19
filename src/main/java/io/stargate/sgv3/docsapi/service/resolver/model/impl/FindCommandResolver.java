package io.stargate.sgv3.docsapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.impl.FindCommand;
import io.stargate.sgv3.docsapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv3.docsapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv3.docsapi.service.operation.model.Operation;
import io.stargate.sgv3.docsapi.service.resolver.model.CommandResolver;
import io.stargate.sgv3.docsapi.service.resolver.model.impl.matcher.FilterableResolver;
import java.util.Optional;
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
  protected Optional<FilteringOptions> getFilteringOption(FindCommand command) {
    int limit =
        command.options() != null && command.options().limit() != 0
            ? command.options().limit()
            : documentConfig.maxLimit();
    int pageSize =
        command.options() != null && command.options().pageSize() != 0
            ? command.options().pageSize()
            : documentConfig.defaultPageSize();
    String pagingState = command.options() != null ? command.options().pagingState() : null;
    return Optional.of(new FilteringOptions(limit, pagingState, pageSize));
  }
}
