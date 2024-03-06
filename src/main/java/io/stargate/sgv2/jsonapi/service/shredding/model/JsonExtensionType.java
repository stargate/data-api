package io.stargate.sgv2.jsonapi.service.shredding.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Type of "extends" JSON scalar type: generally used to retain client-side type information
 * in cases where processing on server-side is still based on enclosed String value.
 */
public enum JsonExtensionType {
    EJSON_DATE("$date"),

    OBJECT_ID("$objectId"),

    UUID("$uuid"),

    UUID_V6("$uuid6"),

    UUID_V7("$uuid7");

    private final String encodedName;

    private JsonExtensionType(String encodedName) {
        this.encodedName = encodedName;
    }

    public String encodedName() { return encodedName; }

    /**
     * @param encodedName Encoded name like {@code $uuid} to find enum value for
     *
     * @return Enum value for given encoded name, if any matching; {@code null} if not
     */
    public static JsonExtensionType fromEncodedName(String encodedName) {
        return Finder.INSTANCE.find(encodedName);
    }

    /**
     * Helper class used to find enum value by encoded name
     */
    static class Finder {
       final static Finder INSTANCE = new Finder();

       private final Map<String, JsonExtensionType> encodedNameMap;

         private Finder() {
         encodedNameMap = new HashMap<>();
            for (JsonExtensionType type : values()) {
                encodedNameMap.put(type.encodedName, type);
            }
        }

        public JsonExtensionType find(String encodedName) {
            return encodedNameMap.get(encodedName);
        }
    }
}
