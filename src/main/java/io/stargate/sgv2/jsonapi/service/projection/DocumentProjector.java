package io.stargate.sgv2.jsonapi.service.projection;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * Helper class that implements functionality needed to support projections on documents fetched via
 * various {@code find} commands.
 */
public class DocumentProjector {
  /** Pseudo-projector that makes no modifications to documents */
  private static final DocumentProjector IDENTITY_PROJECTOR = new DocumentProjector();

  private DocumentProjector() {}

  public static DocumentProjector createFromDefinition(JsonNode projectionDefinition) {
    if (projectionDefinition == null) {
      return identityProjector();
    }
    if (!projectionDefinition.isObject()) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
              + ": definition must be OBJECT, was "
              + projectionDefinition.getNodeType());
    }
    if (projectionDefinition.isEmpty()) {
      return identityProjector();
    }
    PathCollector paths = PathCollector.collectPaths(projectionDefinition);

    return new DocumentProjector();
  }

  public static DocumentProjector identityProjector() {
    return IDENTITY_PROJECTOR;
  }

  public void applyProjection(JsonNode document) {
    ; // To implement
  }

  // Mostly for deserialization tests
  @Override
  public boolean equals(Object o) {
    if (o instanceof DocumentProjector) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return 1;
  }

  /** Helper object used to traverse and collection inclusion/exclusion definitions */
  private static class PathCollector {
    private Set<String> exclusions;
    private Set<String> inclusions;

    private Boolean idInclusion = null;

    private PathCollector() {}

    static PathCollector collectPaths(JsonNode def) {
      return new PathCollector().collectFromObject(def, null);
    }

    PathCollector collectFromObject(JsonNode ob, String parentPath) {
      var it = ob.fields();
      while (it.hasNext()) {
        var entry = it.next();
        String path = entry.getKey();
        if (parentPath != null) {
          path = parentPath + "." + path;
        }
        JsonNode value = entry.getValue();
        if (value.isNumber()) {
          // "0" means exclude (like false); any other number include
          if (BigDecimal.ZERO.equals(value.decimalValue())) {
            addExclusion(path);
          } else {
            addInclusion(path);
          }
        } else if (value.isBoolean()) {
          if (value.booleanValue()) {
            addInclusion(path);
          } else {
            addExclusion(path);
          }
        } else if (value.isObject()) {
          // Nested definitions allowed, too
          collectFromObject(value, path);
        } else {
          // Unknown JSON node type; error
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                  + ": path ('"
                  + path
                  + "') value must be NUMBER, BOOLEAN or OBJECT, was "
                  + value.getNodeType());
        }
      }
      return this;
    }

    private void addExclusion(String path) {
      if (DocumentConstants.Fields.DOC_ID.equals(path)) {
        idInclusion = false;
      } else {
        if (exclusions == null) {
          // Must not mix exclusions and inclusions
          if (inclusions != null) {
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                    + ": cannot exclude '"
                    + path
                    + "' on inclusion projection");
          }
          exclusions = new HashSet<>();
        }
        exclusions.add(path);
      }
    }

    private void addInclusion(String path) {
      if (DocumentConstants.Fields.DOC_ID.equals(path)) {
        idInclusion = true;
      } else {
        if (inclusions == null) {
          // Must not mix exclusions and inclusions
          if (exclusions != null) {
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                    + ": cannot include '"
                    + path
                    + "' on exclusion projection");
          }
          inclusions = new HashSet<>();
        }
        inclusions.add(path);
      }
    }
  }
}
