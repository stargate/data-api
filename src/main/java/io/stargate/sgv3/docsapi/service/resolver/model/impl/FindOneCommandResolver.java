package io.stargate.sgv3.docsapi.service.resolver.model.impl;

import io.stargate.sgv3.docsapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv3.docsapi.service.resolver.model.impl.matcher.FilterableResolver;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;

/** Resolves the {@link FindOneCommand } */
@ApplicationScoped
public class FindOneCommandResolver extends FilterableResolver<FindOneCommand> {
  public FindOneCommandResolver() {
    super(true, true);
  }

  @Override
  public Class<FindOneCommand> getCommandClass() {
    return FindOneCommand.class;
  }

  @Override
  protected Optional<FilteringOptions> getFilteringOption(FindOneCommand command) {
    return Optional.of(new FilteringOptions(1, null));
  }
}
