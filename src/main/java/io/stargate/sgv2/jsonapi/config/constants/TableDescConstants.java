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

  interface ColumnDesc {
    String NAME = "name";
    String TYPE = "type";
    String KEY_TYPE = "keyType";
    String VALUE_TYPE = "valueType";
    String DIMENSION = "dimension";
    String SERVICE = "service";
    String API_SUPPORT = "apiSupport";
  }

  interface PrimaryKey {
    String PARTITION_BY = "partitionBy";
    String PARTITION_SORT = "partitionSort";
  }
}