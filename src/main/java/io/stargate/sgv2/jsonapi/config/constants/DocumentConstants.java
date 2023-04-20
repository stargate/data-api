package io.stargate.sgv2.jsonapi.config.constants;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import java.util.Map;

public interface DocumentConstants {
  /** Names of "special" fields in Documents */
  interface Fields {
    /** Primary key for Documents stored; has special handling for many operations. */
    String DOC_ID = "_id";
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

    Map<Integer, JsonType> keyIDMap =
        Map.of(
            TYPE_ID_STRING,
            JsonType.STRING,
            TYPE_ID_NUMBER,
            JsonType.NUMBER,
            TYPE_ID_BOOLEAN,
            JsonType.BOOLEAN,
            TYPE_ID_NULL,
            JsonType.NULL,
            TYPE_ID_DATE,
            JsonType.DATE);

    static JsonType getJsonType(int typeId) {
      return keyIDMap.get(typeId);
    }
  }
}
