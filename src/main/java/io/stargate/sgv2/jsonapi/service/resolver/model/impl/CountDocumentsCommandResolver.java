package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommands;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link CountDocumentsCommands } */
@ApplicationScoped
public class CountDocumentsCommandResolver extends FilterableResolver<CountDocumentsCommands>
    implements CommandResolver<CountDocumentsCommands> {
  @Inject
  public CountDocumentsCommandResolver(ObjectMapper objectMapper) {
    super(objectMapper);
  }

  @Override
  public Class<CountDocumentsCommands> getCommandClass() {
    return CountDocumentsCommands.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, CountDocumentsCommands command) {
    return resolve(ctx, command);
  }

  @Override
  protected FilteringOptions getFilteringOption(CountDocumentsCommands command) {
    return new FilteringOptions(Integer.MAX_VALUE, null, 1, ReadType.COUNT);
  }
}
