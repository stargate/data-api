package io.stargate.sgv2.jsonapi.api.model.command;

/**
 * The schema object a command can be called against.
 *
 * <p>Example: creteTable runs against the Keyspace , so target is the Keyspace aaron 13 - nove -
 * 2024 - not using the {@link
 * io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject.SchemaObjectType} because this
 * also needs the SYSTEM value, and the schema object design prob needs improvement
 */
public enum CommandTarget {
  COLLECTION,
  TABLE,
  KEYSPACE,
  DATABASE,
  SYSTEM // things like beginOfflineSession that do not work against a schema object
}
