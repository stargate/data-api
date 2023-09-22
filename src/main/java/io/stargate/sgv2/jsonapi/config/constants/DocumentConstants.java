package io.stargate.sgv2.jsonapi.config.constants;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import java.util.regex.Pattern;

public interface DocumentConstants {
  /** Names of "special" fields in Documents */
  interface Fields {
    /** Primary key for Documents stored; has special handling for many operations. */
    String DOC_ID = "_id";

    /** Physical table column name that stores the vector field. */
    String VECTOR_SEARCH_INDEX_COLUMN_NAME = "query_vector_value";

    /** Document field name to which vector data is stored. */
    String VECTOR_EMBEDDING_FIELD = "$vector";

    /** Document field name that will have text value for which vectorize method in called */
    String VECTOR_EMBEDDING_TEXT_FIELD = "$vectorize";

    /** Key for vector function name definition in cql index. */
    String VECTOR_INDEX_FUNCTION_NAME = "similarity_function";

    /** Field name used in projection clause to get similarity score in response. */
    String VECTOR_FUNCTION_PROJECTION_FIELD = "$similarity";

    // Current definition of valid JSON API names: note that this only validates
    // characters, not length limits (nor empty nor "too long" allowed but validated
    // separately)
    Pattern VALID_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_\\-]*");
  }

  interface KeyTypeId {
    /**
     * Type id are used in key stored in database representing the datatype of the id field. These
     * values should not be changed once data is stored in the DB.
     */
    int TYPE_ID_STRING = 1;

    int TYPE_ID_NUMBER = 2;
    int TYPE_ID_BOOLEAN = 3;
    int TYPE_ID_NULL = 4;
    int TYPE_ID_DATE = 5;

    static JsonType getJsonType(int typeId) {
      return switch (typeId) {
        case TYPE_ID_STRING -> JsonType.STRING;
        case TYPE_ID_NUMBER -> JsonType.NUMBER;
        case TYPE_ID_BOOLEAN -> JsonType.BOOLEAN;
        case TYPE_ID_NULL -> JsonType.NULL;
        case TYPE_ID_DATE -> JsonType.DATE;
        default -> null;
      };
    }
  }
}
