package io.stargate.sgv2.jsonapi.api.model.command;

/** Enum class for command types. This is used to categorize commands into DDL and DML. */
public enum CommandType {
  ADMIN(false),
  DDL(true),
  DML(false);

  private final boolean forceSchemaRefresh;

  CommandType(boolean forceSchemaRefresh) {
    this.forceSchemaRefresh = forceSchemaRefresh;
  }

  /**
   * If true, when running this command type any schema cache should be refreshed both before and
   * after executing the command. So force refresh before running, and then evict on the way out.
   *
   * <p>While the backend C* db will send a schema changed message, this is async and so there is no
   * guarantee a single client will see their own schema changes reflected immediately. So also
   * evict on the way out to ensure subsequent commands in the same request see the updated schema.
   *
   * @return <code>true</code> if the schema cache should be refreshed before running, and evicted
   *     after running this command.
   */
  public boolean isForceSchemaRefresh() {
    return forceSchemaRefresh;
  }
}
