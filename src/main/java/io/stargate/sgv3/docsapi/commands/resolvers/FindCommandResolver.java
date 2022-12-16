package io.stargate.sgv3.docsapi.commands.resolvers;

import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.FindCommand;
import java.util.Optional;

/** All the logic moved to {@link FilterableResolver} */
public class FindCommandResolver<T extends Command> extends FilterableResolver<FindCommand> {
  public FindCommandResolver() {
    super(false);
  }

  @Override
  public Optional<FilteringOptions> getFilteringOption(FindCommand command) {
    FindCommand.Options options = command.getOptions();
    if (options != null) {
      return Optional.of(new FilteringOptions(options.limit, options.pagingState));
    } else {
      return Optional.of(new FilteringOptions());
    }
  }
}
