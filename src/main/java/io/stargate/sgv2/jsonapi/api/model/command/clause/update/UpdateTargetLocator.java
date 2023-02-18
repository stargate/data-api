package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.regex.Pattern;

/** Factory for {@link UpdateTarget} instances. */
public class UpdateTargetLocator {
  private static final Pattern DOT = Pattern.compile(Pattern.quote("."));

  private static final Pattern INDEX_SEGMENT = Pattern.compile("0|[1-9][0-9]*");

  /**
   * Method that will create target instance for given path through given document; if no such path
   * exists, will not attempt to create path (nor report any problems).
   *
   * <p>Used for $unset operation.
   *
   * @param document Document that may contain target path
   * @param dotPath Path that points to possibly existing target
   * @return Target instance with optional target and context nodes
   */
  public UpdateTarget findIfExists(JsonNode document, String dotPath) {
    String[] segments = splitAndVerify(dotPath);
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
   * Method that will create target instance for given path through given document; if no such path
   * exists, will try to create path. Creation may fail with an exception for cases like path trying
   * to create properties on Array nodes.
   *
   * <p>Used for update operations that add or modify values (operations other than $unset)
   *
   * @param document Document that is to contain target path
   * @param dotPath Path that points to target that may exists, or is about to be added
   * @return Target instance with optional target and context nodes
   */
  public UpdateTarget findOrCreate(JsonNode document, String dotPath) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private String[] splitAndVerify(String dotPath) {
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
}
