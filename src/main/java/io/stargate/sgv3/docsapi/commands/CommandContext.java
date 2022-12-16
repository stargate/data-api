package io.stargate.sgv3.docsapi.commands;

import io.stargate.sgv3.docsapi.ClientState;

/**
 * Context to run a command in, such as the connected client, the database and collection.
 *
 * <p>e.g. We have a FindOneCommand, the context tells it what DB and table to find on and the
 * {@link ClientState} tells us the tenant the find the cluster and the auth info to use.
 */
public class CommandContext {

  public final String database;
  public final String collection;

  public CommandContext(String database, String collection) {
    this.database = database;
    this.collection = collection;
  }
}
