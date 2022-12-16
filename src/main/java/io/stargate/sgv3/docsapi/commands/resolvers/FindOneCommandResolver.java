package io.stargate.sgv3.docsapi.commands.resolvers;

import io.stargate.sgv3.docsapi.commands.FindOneCommand;

/** All the logic moved to {@link FilterableResolver} */
public class FindOneCommandResolver extends FilterableResolver<FindOneCommand> {

  public FindOneCommandResolver() {
    super(true);
  }
}
