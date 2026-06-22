package io.stargate.sgv2.jsonapi.config.constants;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;

public interface DocumentConstants {
  /** Names of "special" fields in Documents */
  interface Fields {
    /** Primary key for Documents stored; has special handling for many operations. */
    String DOC_ID = "_id";

    /** Document field name to which vector data is stored. */
    String VECTOR_EMBEDDING_FIELD = "$vector";

    /** Document field name that will have text value for which vectorize method in called */
    String VECTOR_EMBEDDING_TEXT_FIELD = "$vectorize";

    /** Document field name that will have lexical (BM-25) content for analyzed text for search */
    String LEXICAL_CONTENT_FIELD = "$lexical";

    /** Document field name that will have text value for which vectorize method in called */
    String BINARY_VECTOR_TEXT_FIELD = "$binary";

    /** Field name used in projection clause to get similarity score in response. */
    String VECTOR_FUNCTION_SIMILARITY_FIELD = "$similarity";

    /**
     * Document field used DOCUMENT_RESPONSE of a findAndRerank scores, so not actually allowed in a
     * document .
     */
    String RERANK_FIELD = "$rerank";

    /** Document field used DOCUMENT_RESPONSE of a findAndRerank scores, sorting, and inserting */
    String HYBRID_FIELD = "$hybrid";

    /** Document field used DOCUMENT_RESPONSE of a findAndRerank scores */
    String SCORES_FIELD = "scores";
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
