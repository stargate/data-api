package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.regex.Pattern;

/** Helper class for resolving "dotted paths" into {@link UpdateTarget} instances. */
public class UpdateTargetLocator {
  private static final Pattern DOT = Pattern.compile(Pattern.quote("."));

  private static final Pattern INDEX_SEGMENT = Pattern.compile("0|[1-9][0-9]*");

  private final String dotPath;

  private final String[] segments;

  private UpdateTargetLocator(String dotPath, String[] segments) {
    this.dotPath = dotPath;
    this.segments = segments;
  }

  public String path() {
    return dotPath;
  }

  /**
   * Factory method for constructing path; also does minimal verification of path: currently only
   * verification is to ensure there are no empty segments.
   *
   * @param dotPath Path that uses dot-notation
   * @return Locator instance
   * @throws JsonApiException if dotPath invalid (empty path segment(s))
   */
  public static UpdateTargetLocator forPath(String dotPath) throws JsonApiException {
    return new UpdateTargetLocator(dotPath, splitAndVerify(dotPath));
  }

  /**
   * Method that will create {@link UpdateTarget} that matches configured path within given
   * document; if no such path exists, will not attempt to create path (nor report any problems) but
   * simply return {@link UpdateTarget} with specific information that is available regarding path.
   *
   * <p>Resulting {@link UpdateTarget} will
   *
   * <p>Used for $unset operation.
   *
   * @param document Document that may contain target path
   */
  public UpdateTarget findIfExists(JsonNode document) {
    JsonNode context = document;
    final int lastSegmentIndex = segments.length - 1;

    // First traverse all but the last segment
    for (int i = 0; i < lastSegmentIndex; ++i) {
      final String segment = segments[i];
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
        return UpdateTarget.missingPath(dotPath);
      }
    }

    // But the last segment is special since we now may get Value node but also need
    // to denote how context refers to it (Object property vs Array index)
    final String segment = segments[lastSegmentIndex];
    if (context.isObject()) {
      return UpdateTarget.pathViaObject(dotPath, context, context.get(segment), segment);
    } else if (context.isArray()) {
      int index = findIndexFromSegment(segment);
      if (index < 0) {
        return UpdateTarget.missingPath(dotPath);
      }
      return UpdateTarget.pathViaArray(dotPath, context, context.get(index), index);
    } else {
      return UpdateTarget.missingPath(dotPath);
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
  public UpdateTarget findOrCreate(JsonNode document) {
    String[] segments = splitAndVerify(dotPath);
    JsonNode context = document;
    final int lastSegmentIndex = segments.length - 1;

    // First traverse all but the last segment
    for (int i = 0; i < lastSegmentIndex; ++i) {
      final String segment = segments[i];
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
    final String segment = segments[lastSegmentIndex];
    if (context.isObject()) {
      return UpdateTarget.pathViaObject(dotPath, context, context.get(segment), segment);
    }
    if (context.isArray()) {
      int index = findIndexFromSegment(segment);
      // Cannot create properties on Arrays
      if (index < 0) {
        throw cantCreatePropertyPath(dotPath, segment, context);
      }
      return UpdateTarget.pathViaArray(dotPath, context, context.get(index), index);
    }
    // Cannot create properties on Atomics either
    throw cantCreatePropertyPath(dotPath, segment, context);
  }

  /**
   * Traversal method that is similar to {@link #findIfExists} but that will not return full {@link
   * UpdateTarget}; instead a non-{@code null} {@link JsonNode} (possibly of type {@code
   * MissingNode} is returned matching value at given path (or lack thereof in case of {@code
   * MissingNode}).
   *
   * @param document Document on which to evaluate configured path.
   * @return Value node in given document at configured path, if any; a "missing node" (one for
   *     which {@code JsonNode.isMissingNode()} returns {@code}).
   */
  public JsonNode findValueIn(JsonNode document) {
    JsonNode context = document;

    // Unlike with other methods, we do not need to use special handling for
    // last segment:
    final int end = segments.length;
    for (int i = 0; i < end; ++i) {
      final String segment = segments[i];
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

  private static String[] splitAndVerify(String dotPath) throws JsonApiException {
    String[] result = DOT.split(dotPath);
    for (String segment : result) {
      if (segment.isEmpty()) {
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                + ": empty segment ('') in path '"
                + dotPath
                + "'");
      }
    }
    return result;
  }

  private int findIndexFromSegment(String segment) {
    if (INDEX_SEGMENT.matcher(segment).matches()) {
      return Integer.parseInt(segment);
    }
    return -1;
  }

  private JsonApiException cantCreatePropertyPath(String fullPath, String prop, JsonNode context) {
    return new JsonApiException(
        ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH,
        String.format(
            "%s: cannot create field ('%s') in path '%s'; only OBJECT nodes have properties (got %s)",
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage(),
            prop,
            fullPath,
            context.getNodeType()));
  }
}
