package io.stargate.sgv2.jsonapi.config.constants;

public interface TableDescConstants {

  interface TableDesc {
    String NAME = "name";
    String DEFINITION = "definition";
  }

  interface TableDefinitionDesc {
    String COLUMNS = "columns";
    String PRIMARY_KEY = "primaryKey";
  }

  interface PrimaryKey {
    String PARTITION_BY = "partitionBy";
    String PARTITION_SORT = "partitionSort";
  }
}
