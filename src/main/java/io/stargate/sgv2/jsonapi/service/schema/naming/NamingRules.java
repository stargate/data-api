package io.stargate.sgv2.jsonapi.service.schema.naming;

/**
 * This class serves as a centralized repository for naming rules that validate the names of
 * different components, such as keyspaces, collections, tables, and indexes.
 */
public final class NamingRules {
  private NamingRules() {}

  public static final SchemaObjectNamingRule KEYSPACE = new KeyspaceNamingRule();
  public static final SchemaObjectNamingRule COLLECTION = new CollectionNamingRule();
  public static final SchemaObjectNamingRule TABLE = new TableNamingRule();
  public static final SchemaObjectNamingRule INDEX = new IndexNamingRule();
}
