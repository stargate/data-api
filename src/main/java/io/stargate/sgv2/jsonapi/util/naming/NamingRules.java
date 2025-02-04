package io.stargate.sgv2.jsonapi.util.naming;

public final class NamingRules {
  private NamingRules() {}

  public static final SchemaObjectNamingRule KEYSPACE = new KeyspaceNamingRule();
  public static final SchemaObjectNamingRule COLLECTION = new CollectionNamingRule();
  public static final SchemaObjectNamingRule TABLE = new TableNamingRule();
  public static final SchemaObjectNamingRule INDEX = new IndexNamingRule();
}
