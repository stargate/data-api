package io.stargate.sgv3.docsapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv3.docsapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv3.docsapi.service.resolver.model.impl.matcher.FilterableResolver;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link FindOneCommand } */
@ApplicationScoped
public class FindOneCommandResolver extends FilterableResolver<FindOneCommand> {

  @Inject
  public FindOneCommandResolver(ObjectMapper objectMapper) {
    super(objectMapper, true, true);
  }

  public FindOneCommandResolver() {
    this(null);
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
