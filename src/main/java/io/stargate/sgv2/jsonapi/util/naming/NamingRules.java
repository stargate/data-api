package io.stargate.sgv2.jsonapi.util.naming;

public final class NamingRules {
  private NamingRules() {}

  public static final SchemaObjectNamingRule KEYSPACE = new KeyspaceNamingRule("keyspace");
  public static final SchemaObjectNamingRule COLLECTION = new CollectionNamingRule("collection");
  public static final SchemaObjectNamingRule TABLE = new TableNamingRule("table");
  public static final SchemaObjectNamingRule INDEX = new IndexNamingRule("index");
}
