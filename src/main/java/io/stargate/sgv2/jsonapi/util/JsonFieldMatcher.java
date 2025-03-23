package io.stargate.sgv2.jsonapi.util;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import java.util.function.Predicate;

/**
 * Matches fields from a {@link JsonNode}, returning a {@link MatchResult} so the caller to see
 * which fields matched, and which fields were unexpected. Use with a deserialser to make it easy to
 * get he expected fields, and those that were not expected.
 *
 * <p>Two uses:
 *
 * <ul>
 *   <li>Create a {@link JsonFieldMatcher} with the expected fields, and then call {@link
 *       JsonFieldMatcher#match(JsonNode)}
 *   <li>Call {@link JsonFieldMatcher#match(Class, JsonNode, List)} directly
 * </ul>
 *
 * @param type The sublcass of {@link JsonNode} that all fo the expected fields should be, often
 *     this will just be {@link JsonNode}, but could be a more specific type such as {@link
 *     com.fasterxml.jackson.databind.node.TextNode}.
 * @param requiredFields List of the field names that are required in the JSON object.
 * @param optionalFields List of the field names that are optional in the JSON object.
 * @param <T> Type for the {@param type} parameter.
 */
public record JsonFieldMatcher<T extends JsonNode>(
    Class<T> type, List<String> requiredFields, List<String> optionalFields) {

  public JsonFieldMatcher {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(requiredFields, "requiredFields must not be null");
    Objects.requireNonNull(optionalFields, "optionalFields must not be null");
  }

  /**
   * Match the fields in the {@param node} with the expected fields in this instance.
   *
   * @param node The JSON object to match.
   * @return A {@link MatchResult} that contains the fields that matched, the fields that were of
   *     the wrong type,
   */
  public MatchResult<T> match(JsonNode node) {
    return match(type, node, requiredFields, optionalFields);
  }

  public MatchResult<T> matchAndThrow(JsonNode node, JsonParser jsonParser, String context)
      throws JsonMappingException {
    return match(type, node, requiredFields, optionalFields).throwIfInvalid(jsonParser, context);
  }

  public static JsonMappingException errorForWrongType(
      JsonParser jsonParser,
      String context,
      String fieldName,
      JsonNode invalidNode,
      Class<?>... allowedTypes) {
    return new JsonMappingException(
        jsonParser,
        "%s contained fields with the wrong type. Field %s may only be of types %s, but got: %s"
            .formatted(
                context,
                fieldName,
                String.join(", ", Arrays.stream(allowedTypes).map(Class::getSimpleName).toList()),
                classSimpleName(
                    invalidNode.getClass())), // the type ENUM is different to the class names above
        jsonParser.currentLocation());
  }

  /**
   * Match the fields in the {@param node} with the expected fields.
   *
   * <p>
   *
   * @param clazz Same as {@link JsonFieldMatcher} docs
   * @param node The JSON node to match against
   * @param requiredFields Same as {@link JsonFieldMatcher}
   * @param optionalFields Same as {@link JsonFieldMatcher}
   * @return A {@link MatchResult} that contains the fields that matched, the fields that were of
   *     the wrong clazz,
   * @param <T> Type for the {@param clazz} parameter.
   */
  public static <T extends JsonNode> MatchResult<T> match(
      Class<T> clazz, JsonNode node, List<String> requiredFields, List<String> optionalFields) {

    Map<String, T> matched = null;
    Map<String, JsonNode> wrongType = null;
    Map<String, JsonNode> unexpectedFields = null;
    List<String> missingFields = null;

    // Create a set for faster lookups if there are enough fields to match
    Predicate<String> required =
        requiredFields.size() > 3
            ? new HashSet<>(requiredFields)::contains
            : requiredFields::contains;

    Predicate<String> optional =
        requiredFields.size() > 3
            ? new HashSet<>(optionalFields)::contains
            : optionalFields::contains;

    var unseenRequired = new HashSet<>(requiredFields);

    for (var entry : node.properties()) {

      unseenRequired.remove(entry.getKey());

      if (!required.test(entry.getKey()) && !optional.test(entry.getKey())) {
        if (unexpectedFields == null) {
          unexpectedFields = new HashMap<>();
        }
        unexpectedFields.put(entry.getKey(), entry.getValue());
      } else {
        if (clazz.isInstance(entry.getValue())) {
          if (matched == null) {
            // only setting the size for matching, as we assume that is the most likely.
            matched = new HashMap<>(node.properties().size());
          }
          // clazz checked above
          matched.put(entry.getKey(), unchecked(entry.getValue()));
        } else {
          if (wrongType == null) {
            wrongType = new HashMap<>();
          }
          wrongType.put(entry.getKey(), entry.getValue());
        }
      }
    }

    var expectedFields = new ArrayList<>(requiredFields);
    expectedFields.addAll(optionalFields);

    return new MatchResult<>(
        clazz,
        matched == null ? Map.of() : matched,
        wrongType == null ? Map.of() : wrongType,
        unexpectedFields == null ? Map.of() : unexpectedFields,
        List.copyOf(unseenRequired),
        expectedFields);
  }

  public static <T extends JsonNode> MatchResult<T> matchAndThrow(
      Class<T> type,
      JsonNode node,
      List<String> expectedFields,
      List<String> optionalFields,
      JsonParser jsonParser,
      String context)
      throws JsonMappingException {
    return match(type, node, expectedFields, optionalFields).throwIfInvalid(jsonParser, context);
  }

  @SuppressWarnings("unchecked")
  private static <T extends JsonNode> T unchecked(JsonNode node) {
    return (T) node;
  }

  /**
   * Results of matching the fields in a JSON node.
   *
   * @param matched Map of expected fields names to node as the <code>T</code>
   * @param wrongType Map field names that were expected, but had the wrong type, to the {@link
   *     JsonNode}.
   * @param unexpected Map of field names that were not expected, to the {@link JsonNode}.
   * @param expectedFields The list of field names that were expected.
   * @param <T> Type of the {@link JsonNode} subclass that covers all expected fields.
   */
  public record MatchResult<T extends JsonNode>(
      Class<T> type,
      Map<String, T> matched,
      Map<String, JsonNode> wrongType,
      Map<String, JsonNode> unexpected,
      List<String> missingFields,
      List<String> expectedFields) {

    public MatchResult {
      Objects.requireNonNull(type, "type must not be null");
      Objects.requireNonNull(matched, "matched must not be null");
      Objects.requireNonNull(wrongType, "wrongType must not be null");
      Objects.requireNonNull(unexpected, "unexpected must not be null");
      Objects.requireNonNull(missingFields, "missingFields must not be null");
      Objects.requireNonNull(expectedFields, "requiredFields must not be null");
    }

    /** Fields that had an unexpected name or type. */
    public List<String> invalidFields() {
      if (unexpected.isEmpty() && wrongType.isEmpty()) {
        return List.of();
      }
      List<String> list = new ArrayList<>(unexpected.keySet());
      list.addAll(wrongType.keySet());
      return list;
    }

    public MatchResult<T> throwIfInvalid(JsonParser jsonParser, String context)
        throws JsonMappingException {

      if (!unexpected().isEmpty()) {
        throw new JsonMappingException(
            jsonParser,
            "%s contained unexpected fields. Expected fields: %s. Unexpected fields: %s"
                .formatted(
                    context, String.join(", ", expectedFields), String.join(", ", invalidFields())),
            jsonParser.currentLocation());
      }

      if (!wrongType().isEmpty()) {
        throw new JsonMappingException(
            jsonParser,
            "%s contained fields with the wrong type. Expected fields: %s to all be of type: %s. Wrong type fields: %s"
                .formatted(
                    context,
                    String.join(", ", expectedFields),
                    getClass().getSimpleName(),
                    String.join(", ", wrongType.keySet())),
            jsonParser.currentLocation());
      }

      if (!missingFields.isEmpty()) {
        throw new JsonMappingException(
            jsonParser,
            "%s has missing fields. Expected fields: %s. Missing fields: %s"
                .formatted(
                    context, String.join(", ", expectedFields), String.join(", ", missingFields)),
            jsonParser.currentLocation());
      }
      return this;
    }
  }
}
