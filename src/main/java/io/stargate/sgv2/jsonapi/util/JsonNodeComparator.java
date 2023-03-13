package io.stargate.sgv2.jsonapi.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 * {@link Comparator} for sorting {@link JsonNode} values as needed for operations like {@code $min}
 * and {@code $max}. Uses definitions of BSON type-based sorting, where order of types is (from
 * lowest to highest precedence):
 *
 * <ol>
 *   <li>Null
 *   <li>Number
 *   <li>String
 *   <li>Object
 *   <li>Array
 *   <li>Boolean
 * </oL>
 *
 * (NOTE: these are types we have -- MongoDB has more native types so this is a subset of BSON
 * sorting definitions).
 *
 * <p>Within each type sorting is as usual for most types (Numbers, Strings, Booleans). Arrays use
 * straight-forward element-by-element sorting (similar to Strings). The only more esoteric case are
 * Objects, where sorting is by ordered fields, first comparing field name (String sort), if same,
 * then recursively by value; and if first N fields the same, Object with more properties is sorted
 * last.
 */
public class JsonNodeComparator implements Comparator<JsonNode> {
  private static final Comparator<JsonNode> ASC = new JsonNodeComparator();

  private static final Comparator<JsonNode> DESC = ASC.reversed();

  public static Comparator<JsonNode> ascending() {
    return ASC;
  }

  public static Comparator<JsonNode> descending() {
    return DESC;
  }

  @Override
  public int compare(JsonNode o1, JsonNode o2) {
    JsonNodeType type1 = o1.getNodeType();
    JsonNodeType type2 = o2.getNodeType();

    // If value types differ, base on type precedence as per Mongo specs:
    if (type1 != type2) {
      return typePriority(type1) - typePriority(type2);
    }

    switch (type1) {
      case MISSING:
      case NULL:
        return 0; // nulls and missing are equal to each other so...
      case NUMBER:
        return compareNumbers(o1.decimalValue(), o2.decimalValue());
      case STRING:
        return compareStrings(o1.textValue(), o2.textValue());
      case OBJECT:
        return compareObjects((ObjectNode) o1, (ObjectNode) o2);
      case ARRAY:
        return compareArrays((ArrayNode) o1, (ArrayNode) o2);
      case BOOLEAN:
        return compareBooleans(o1.booleanValue(), o2.booleanValue());
      default:
        // Should never happen:
        throw new IllegalStateException("Unsupported JsonNodeType for comparison: " + type1);
    }
  }

  private int compareBooleans(boolean b1, boolean b2) {
    if (b1 == b2) {
      return 0;
    }
    return b1 ? 1 : -1;
  }

  private int compareNumbers(BigDecimal n1, BigDecimal n2) {
    return n1.compareTo(n2);
  }

  private int compareStrings(String n1, String n2) {
    return n1.compareTo(n2);
  }

  private int compareArrays(ArrayNode n1, ArrayNode n2) {
    final int len1 = n1.size();
    final int len2 = n2.size();

    // First: compare first N entries that are common
    for (int i = 0, end = Math.min(len1, len2); i < end; ++i) {
      int diff = compare(n1.get(i), n2.get(i));
      if (diff != 0) {
        return diff;
      }
    }

    // and if no difference, longer Array has higher precedence
    return len1 - len2;
  }

  private int compareObjects(ObjectNode n1, ObjectNode n2) {
    // Object comparison is interesting: compares entries in order,
    // first by property name, then by value. If all else equal, "longer one wins"
    Iterator<Map.Entry<String, JsonNode>> it1 = n1.fields();
    Iterator<Map.Entry<String, JsonNode>> it2 = n2.fields();

    while (it1.hasNext() && it2.hasNext()) {
      Map.Entry<String, JsonNode> entry1 = it1.next();
      Map.Entry<String, JsonNode> entry2 = it2.next();

      // First, key:
      int diff = entry1.getKey().compareTo(entry2.getKey());
      if (diff == 0) {
        // If key same, then value
        diff = compare(entry1.getValue(), entry2.getValue());
        if (diff == 0) {
          continue;
        }
      }
      return diff;
    }

    // Longer one wins, otherwise
    return n1.size() - n2.size();
  }

  private int typePriority(JsonNodeType type) {
    switch (type) {
      case MISSING:
        return 0;
      case NULL:
        return 1;
      case NUMBER:
        return 2;
      case STRING:
        return 3;
      case OBJECT:
        return 4;
      case ARRAY:
        return 5;
      case BOOLEAN:
        return 6;
      default:
        return 7;
    }
  }
}
