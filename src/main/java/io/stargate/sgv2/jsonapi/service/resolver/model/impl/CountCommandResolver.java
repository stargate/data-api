package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link CountCommand } */
@ApplicationScoped
public class CountCommandResolver extends FilterableResolver<CountCommand>
    implements CommandResolver<CountCommand> {
  @Inject
  public CountCommandResolver(ObjectMapper objectMapper) {
    super(objectMapper);
  }

  public CountCommandResolver() {
    this(null);
  }

  @Override
  public Class<CountCommand> getCommandClass() {
    return CountCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, CountCommand command) {
    return resolve(ctx, command);
  }

  @Override
  protected FilteringOptions getFilteringOption(CountCommand command) {
    return new FilteringOptions(Integer.MAX_VALUE, null, 1, ReadType.COUNT);
  }
}
