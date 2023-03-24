package io.stargate.sgv2.jsonapi.service.projection;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Helper class that implements functionality needed to support projections on documents fetched via
 * various {@code find} commands.
 */
public class DocumentProjector {
  /** Pseudo-projector that makes no modifications to documents */
  private static final DocumentProjector IDENTITY_PROJECTOR = new DocumentProjector(null, true);

  private final ProjectionLayer rootLayer;

  /** Whether this projector is inclusion- ({@code true}) or exclusion ({@code false}) based. */
  private final boolean inclusion;

  private DocumentProjector(ProjectionLayer rootLayer, boolean inclusion) {
    this.rootLayer = rootLayer;
    this.inclusion = inclusion;
  }

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
    return PathCollector.collectPaths(projectionDefinition).buildProjector();
  }

  public static DocumentProjector identityProjector() {
    return IDENTITY_PROJECTOR;
  }

  public void applyProjection(JsonNode document) {
    if (rootLayer != null) { // null -> identity projection (no-op)
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM, "Non-identity Projections not yet supported");
    }
  }

  // Mostly for deserialization tests
  @Override
  public boolean equals(Object o) {
    if (o instanceof DocumentProjector) {
      DocumentProjector other = (DocumentProjector) o;
      return (this.inclusion == other.inclusion) && Objects.equals(this.rootLayer, other.rootLayer);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return rootLayer.hashCode();
  }

  /**
   * Helper object used to traverse and collection inclusion/exclusion path definitions and verify
   * that there are only one or the other (except for doc id). Does not build data structures for
   * actual matching.
   */
  private static class PathCollector {
    private List<String> paths = new ArrayList<>();

    private int exclusions, inclusions;

    private Boolean idInclusion = null;

    private PathCollector() {}

    static PathCollector collectPaths(JsonNode def) {
      return new PathCollector().collectFromObject(def, null);
    }

    public DocumentProjector buildProjector() {
      if (isIdentityProjection()) {
        return DocumentProjector.identityProjector();
      }

      return new DocumentProjector(ProjectionLayer.buildLayers(paths), inclusions > 0);
    }

    /**
     * Accessor to use for checking if collected paths indicate "empty" (no-operation) projection:
     * if so, caller can avoid actual construction or evaluation.
     */
    boolean isIdentityProjection() {
      // Only the case if we have no non-doc-id inclusions/exclusions AND
      // doc-id is included (by default or explicitly)
      return paths.isEmpty() && !Boolean.FALSE.equals(idInclusion);
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
        // Must not mix exclusions and inclusions
        if (inclusions > 0) {
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                  + ": cannot exclude '"
                  + path
                  + "' on inclusion projection");
        }
        ++exclusions;
        paths.add(path);
      }
    }

    private void addInclusion(String path) {
      if (DocumentConstants.Fields.DOC_ID.equals(path)) {
        idInclusion = true;
      } else {
        // Must not mix exclusions and inclusions
        if (exclusions > 0) {
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                  + ": cannot include '"
                  + path
                  + "' on exclusion projection");
        }
        ++inclusions;
        paths.add(path);
      }
    }
  }

  /**
   * Helper class that handles projection traversal for one level of nesting. Layers are either
   * non-terminal (branches) or terminal (leaves)
   */
  private static class ProjectionLayer {
    private static final Pattern DOT = Pattern.compile(Pattern.quote("."));

    /** Whether this layer is terminal (matching) or branch (non-matching) */
    private final boolean isTerminal;

    /**
     * Full path either to this layer (terminal), or to the first path through it (non-terminal) --
     * needed for conflict reporting.
     */
    private String fullPath;

    /** For non-terminal layers, segment-indexed next layers */
    private final Map<String, ProjectionLayer> nextLayers;

    ProjectionLayer(boolean terminal, String fullPath) {
      isTerminal = terminal;
      this.fullPath = fullPath;
      nextLayers = isTerminal ? null : new HashMap<>();
    }

    public static ProjectionLayer buildLayers(Collection<String> dotPaths) {
      // Root is always branch (not terminal):
      ProjectionLayer root = new ProjectionLayer(false, "");
      for (String fullPath : dotPaths) {
        String[] segments = DOT.split(fullPath);
        buildPath(fullPath, root, segments);
      }
      return root;
    }

    static void buildPath(String fullPath, ProjectionLayer layer, String[] segments) {
      // First create branches
      final int last = segments.length - 1;
      for (int i = 0; i < last; ++i) {
        // Try to find or create branch
        layer = layer.findOrCreateBranch(fullPath, segments[i]);
      }
      // And then attach terminal (leaf)
      layer.addTerminal(fullPath, segments[last]);
    }

    ProjectionLayer findOrCreateBranch(String fullPath, String segment) {
      // Cannot proceed past terminal layer (shorter path):
      if (isTerminal) {
        reportPathConflict(this.fullPath, fullPath);
      }
      ProjectionLayer next = nextLayers.get(segment);
      if (next == null) {
        next = new ProjectionLayer(false, fullPath);
        nextLayers.put(segment, next);
      }
      return next;
    }

    void addTerminal(String fullPath, String segment) {
      // Cannot proceed past terminal layer (shorter path):
      if (isTerminal) {
        reportPathConflict(this.fullPath, fullPath);
      }
      // But will also not allow existing longer path:
      ProjectionLayer next = nextLayers.get(segment);
      if (next != null) {
        reportPathConflict(fullPath, next.fullPath);
      }
      nextLayers.put(segment, new ProjectionLayer(true, fullPath));
    }

    void reportPathConflict(String fullPath1, String fullPath2) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
              + ": projection path conflict between '"
              + fullPath1
              + "' and '"
              + fullPath2
              + "'");
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof ProjectionLayer)) return false;
      ProjectionLayer other = (ProjectionLayer) o;
      return (this.isTerminal == other.isTerminal)
          && Objects.equals(this.fullPath, other.fullPath)
          && Objects.equals(this.nextLayers, other.nextLayers);
    }

    @Override
    public int hashCode() {
      return Objects.hash(isTerminal, fullPath, nextLayers);
    }
  }
}
