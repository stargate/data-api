package io.stargate.sgv2.jsonapi.config.constants;

import java.util.List;

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
    String UDT_NAME = "udtName";
    String DEFINITION = "definition";
    String FIELDS = "fields";

    /**
     * Helper method for checked whether given field name refers to valid column definition input
     * field or not.
     *
     * @param fname Given field name
     * @return true If given field name is a valid main-level column definition input field; false
     *     if not
     */
    static boolean isKnownDefinitionInputField(String fname) {
      return switch (fname) {
        case TYPE, KEY_TYPE, VALUE_TYPE, DIMENSION, SERVICE -> true;
        default -> false;
      };
    }

    /**
     * Accessor for a set of known fields for which {@link #isKnownDefinitionInputField(String)}
     * would return true.
     */
    static List<String> getKnownDefinitionInputFields() {
      return List.of(TYPE, KEY_TYPE, VALUE_TYPE, DIMENSION, SERVICE);
    }
  }

  interface PrimaryKey {
    String PARTITION_BY = "partitionBy";
    String PARTITION_SORT = "partitionSort";
  }

  interface IndexDesc {
    String DEFINITION = "definition";
    String INDEX_TYPE = "indexType";
    String NAME = "name";
    String OPTIONS = "options";
  }

  interface IndexDefinitionDesc {
    String COLUMN = "column";
    String OPTIONS = "options";
    String API_SUPPORT = "apiSupport";
  }

  interface RegularIndexDefinitionDescOptions {
    String ASCII = "ascii";
    String CASE_SENSITIVE = "caseSensitive";
    String NORMALIZE = "normalize";
  }

  interface TextIndexDefinitionDescOptions {
    String ANALYZER = "analyzer";
  }

  /** Options for the creating text index via CQL. */
  interface TextIndexCQLOptions {
    String OPTION_ANALYZER = "index_analyzer";
  }

  interface VectorIndexDefinitionDescOptions {
    String SOURCE_MODEL = "source_model";
    String SIMILARITY_FUNCTION = "similarity_function";
  }

  // These strings will be used in createIndex, filtering for map datatype
  interface MapTypeComponent {
    String keys = "$keys";
    String values = "$values";
  }
}
