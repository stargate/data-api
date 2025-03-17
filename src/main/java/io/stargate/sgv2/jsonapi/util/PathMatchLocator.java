package io.stargate.sgv2.jsonapi.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;
import java.util.regex.Pattern;

/**
 * Helper class built from "dotted paths" and evaluated in context of a JSON document to produce one
 * of:
 *
 * <ul>
 *   <li>{@link PathMatch} instances for Document updates
 *   <li>{@link JsonNode} when extracting value using {@link #findValueIn} (used for Sorting and
 *       possibly Projection)
 * </ul>
 *
 * <p>Implements {@link Comparable} so that locators are naturally sorted in "Segment-aware" way:
 * meaning that sorting is segment-by-segment, alphabetically, so that parent path will be sorted
 * immediately before its first child path (if any). This sorting is used to verify that update
 * operations' locator paths do not overlap in ancestors/descendants dimensions.
 */
public class PathMatchLocator implements Comparable<PathMatchLocator> {
  private static final Pattern INDEX_SEGMENT = Pattern.compile("0|[1-9][0-9]*");

  private final String dotPath;

  private final DocumentPath segments;

  private PathMatchLocator(String dotPath, DocumentPath segments) {
    this.dotPath = dotPath;
    this.segments = segments;
  }

  public String path() {
    return dotPath;
  }

  /**
   * Method that will check whether this locator represents a "sub-path" of given locator: this is
   * the case if the "parent" path is a proper prefix of this path, followed by a comma and path
   * segment(s). For example: if this locator has path {@code a.b.c} and {@code possibleParent} has
   * path {@code a.b} then method would return true (as suffix is {@code .c}).
   *
   * <p>Note: if paths are the same, will NOT be considered a sub-path (returns {@code false}).
   *
   * @param possibleParent Locator to check against
   * @return True if this locator has a path that is sub-path of path of {@code possibleParent}
   */
  public boolean isSubPathOf(PathMatchLocator possibleParent) {
    String parentPath = possibleParent.path();
    String thisPath = path();
    final int parentLen = parentPath.length();

    return thisPath.startsWith(parentPath)
        && parentLen < thisPath.length()
        && thisPath.charAt(parentLen) == '.';
  }

  /**
   * Factory method for constructing path; also does minimal verification of path: currently only
   * verification is to ensure there are no empty segments.
   *
   * @param dotPath Path that uses dot-notation
   * @return Locator instance
   * @throws JsonApiException if dotPath invalid (empty path segment(s))
   */
  public static PathMatchLocator forPath(String dotPath) throws JsonApiException {
    return new PathMatchLocator(dotPath, splitAndVerify(dotPath));
  }

  /**
   * Method that will create {@link PathMatch} that matches configured path within given document;
   * if no such path exists, will not attempt to create path (nor report any problems) but simply
   * return {@link PathMatch} with specific information that is available regarding path.
   *
   * <p>Resulting {@link PathMatch} will
   *
   * <p>Used for $unset operation.
   *
   * @param document Document that may contain target path
   */
  public PathMatch findIfExists(JsonNode document) {
    JsonNode context = document;
    final int lastSegmentIndex = segments.getSegmentsSize() - 1;

    // First traverse all but the last segment
    for (int i = 0; i < lastSegmentIndex; ++i) {
      final String segment = segments.getSegment(i);
      // Simple logic: Object nodes traversed via property; Arrays index; others can't
      if (context.isObject()) {
        context = context.get(segment);
      } else if (context.isArray()) {
        int index = findIndexFromSegment(segment);
        // Arrays MUST be accessed via index but here mismatch will not result
        // in exception (as having path is optional).
        context = (index < 0) ? null : context.get(index);
      } else {
        context = null;
      }
      if (context == null) {
        return PathMatch.missingPath(dotPath);
      }
    }

    // But the last segment is special since we now may get Value node but also need
    // to denote how context refers to it (Object property vs Array index)
    final String segment = segments.getSegment(lastSegmentIndex);
    if (context.isObject()) {
      return PathMatch.pathViaObject(dotPath, context, context.get(segment), segment);
    } else if (context.isArray()) {
      int index = findIndexFromSegment(segment);
      if (index < 0) {
        return PathMatch.missingPath(dotPath);
      }
      return PathMatch.pathViaArray(dotPath, context, context.get(index), index);
    } else {
      return PathMatch.missingPath(dotPath);
    }
  }

  /**
   * Method that will create target instance using configured path through given document; if no
   * such path exists, will try to create path. Creation may fail with an exception for cases like
   * path trying to create properties on Array nodes.
   *
   * <p>Used for update operations that add or modify values (operations other than $unset)
   *
   * @param document Document that is to contain target path
   */
  public PathMatch findOrCreate(JsonNode document) {
    DocumentPath segments = splitAndVerify(dotPath);
    JsonNode context = document;
    final int lastSegmentIndex = segments.getSegmentsSize() - 1;

    // First traverse all but the last segment
    for (int i = 0; i < lastSegmentIndex; ++i) {
      final String segment = segments.getSegment(i);
      JsonNode nextContext;

      // Simple logic: Object nodes traversed via property; Arrays index; others can't
      if (context.isObject()) {
        nextContext = context.get(segment);
        if (nextContext == null) {
          nextContext = ((ObjectNode) context).putObject(segment);
        }
      } else if (context.isArray()) {
        int index = findIndexFromSegment(segment);
        // Arrays MUST be accessed via index but here mismatch will not result
        // in exception (as having path is optional).
        if (index < 0) {
          throw cantCreatePropertyPath(dotPath, segment, context);
        }
        // Ok; either existing path (within existing array)
        ArrayNode array = (ArrayNode) context;
        nextContext = context.get(index);
        // Or, if not within, then need to create, including null padding
        if (nextContext == null) {
          // Fill up padding up to -- but NOT INCLUDING -- position to add
          while (array.size() < index) {
            array.addNull();
          }
          // Also: must assume Object to add, no way to induce "missing" Arrays
          nextContext = ((ArrayNode) context).addObject();
        }
      } else {
        throw cantCreatePropertyPath(dotPath, segment, context);
      }
      context = nextContext;
    }

    // But the last segment is special since we now may get Value node but also need
    // to denote how context refers to it (Object property vs Array index)
    final String segment = segments.getSegment(lastSegmentIndex);
    if (context.isObject()) {
      return PathMatch.pathViaObject(dotPath, context, context.get(segment), segment);
    }
    if (context.isArray()) {
      int index = findIndexFromSegment(segment);
      // Cannot create properties on Arrays
      if (index < 0) {
        throw cantCreatePropertyPath(dotPath, segment, context);
      }
      return PathMatch.pathViaArray(dotPath, context, context.get(index), index);
    }
    // Cannot create properties on Atomics either
    throw cantCreatePropertyPath(dotPath, segment, context);
  }

  /**
   * Traversal method that is similar to {@link #findIfExists} but that will not return full {@link
   * PathMatch}; instead a non-{@code null} {@link JsonNode} (possibly of type {@code MissingNode}
   * is returned matching value at given path (or lack thereof in case of {@code MissingNode}).
   *
   * @param document Document on which to evaluate configured path.
   * @return Value node in given document at configured path, if any; a "missing node" (one for
   *     which {@code JsonNode.isMissingNode()} returns {@code}).
   */
  public JsonNode findValueIn(JsonNode document) {
    JsonNode context = document;

    // Unlike with other methods, we do not need to use special handling for
    // last segment:
    final int end = segments.getSegmentsSize();
    for (int i = 0; i < end; ++i) {
      final String segment = segments.getSegment(i);
      int index;
      if (context.isArray() && (index = findIndexFromSegment(segment)) >= 0) {
        context = context.path(index);
      } else {
        context = context.path(segment);
      }
      // Short-circuit if no such path (no need for further traversal)
      if (context.isMissingNode()) {
        break;
      }
    }
    return context;
  }

  private static DocumentPath splitAndVerify(String dotPath) throws JsonApiException {
    DocumentPath path;
    try {
      path = DocumentPath.from(dotPath);
    } catch (IllegalArgumentException e) {
      throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PATH.toApiException(
          "update path ('%s') is not a valid path. " + e.getMessage(), dotPath);
    }
    return path;
  }

  private int findIndexFromSegment(String segment) {
    if (INDEX_SEGMENT.matcher(segment).matches()) {
      return Integer.parseInt(segment);
    }
    return -1;
  }

  private JsonApiException cantCreatePropertyPath(String fullPath, String prop, JsonNode context) {
    return ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PATH.toApiException(
        "cannot create field ('%s') in path '%s'; only OBJECT nodes have properties (got %s)",
        prop, fullPath, context.getNodeType());
  }

  // Needed because Command Resolver unit tests rely on equality checks for Command equality
  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof PathMatchLocator)) return false;
    return dotPath.equals(((PathMatchLocator) o).dotPath);
  }

  @Override
  public int hashCode() {
    return dotPath.hashCode();
  }

  @Override
  public String toString() {
    return dotPath;
  }

  @Override
  public int compareTo(PathMatchLocator other) {
    return segments.compareTo(other.segments);
  }
}
