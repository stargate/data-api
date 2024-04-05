package io.stargate.sgv2.jsonapi.service.projection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Helper class that handles projection traversal for one level of nesting. Layers are either
 * non-terminal (branches) or terminal (leaves)
 */
class ProjectionLayer {
  private static final Pattern DOT = Pattern.compile(Pattern.quote("."));

  /** Whether this layer is terminal (matching) or branch (non-matching) */
  private final boolean isTerminal;

  /** In case of {@code $slice} operation, helper object that implements slicing logic. */
  private final Slicer slicer;

  /**
   * Full path either to this layer (terminal), or to the first path through it (non-terminal) --
   * needed for conflict reporting.
   */
  private final String fullPath;

  /** For non-terminal layers, segment-indexed next layers */
  private final Map<String, ProjectionLayer> nextLayers;

  ProjectionLayer(String fullPath, boolean terminal) {
    this.fullPath = fullPath;
    isTerminal = terminal;
    slicer = null;
    nextLayers = isTerminal ? null : new HashMap<>();
  }

  ProjectionLayer(String fullPath, Slicer slicer) {
    this.fullPath = fullPath;
    this.slicer = slicer;
    isTerminal = true;
    nextLayers = null;
  }

  public static ProjectionLayer buildLayersForProjection(
      Collection<String> dotPaths,
      List<SliceDef> slices,
      boolean addDocId,
      boolean add$vector,
      boolean add$vectorize) {
    return buildLayers(dotPaths, slices, true, addDocId, add$vector, add$vectorize);
  }

  public static ProjectionLayer buildLayersForIndexing(Collection<String> dotPaths) {
    return buildLayers(dotPaths, Collections.emptyList(), false, false, false, false);
  }

  private static ProjectionLayer buildLayers(
      Collection<String> dotPaths,
      List<SliceDef> slices,
      boolean failOnOverlap,
      boolean addDocId,
      boolean add$vector,
      boolean add$vectorize) {
    // Root is always branch (not terminal):
    ProjectionLayer root = new ProjectionLayer("", false);
    for (String fullPath : dotPaths) {
      String[] segments = DOT.split(fullPath);
      buildPath(failOnOverlap, fullPath, root, segments);
    }
    // Slices similar to path but processed differently (and while "exclusions"
    // in a way do not count as ones wrt compatibility)
    if (slices != null) {
      for (SliceDef slice : slices) {
        buildSlicer(failOnOverlap, slice, root);
      }
    }

    // May need to add doc-id inclusion/exclusion as well
    if (addDocId) {
      buildPath(
          failOnOverlap,
          DocumentConstants.Fields.DOC_ID,
          root,
          new String[] {DocumentConstants.Fields.DOC_ID});
    }
    if (add$vector) {
      buildPath(
          failOnOverlap,
          DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD,
          root,
          new String[] {DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD});
    }
    if (add$vectorize) {
      buildPath(
          failOnOverlap,
          DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
          root,
          new String[] {DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD});
    }
    return root;
  }

  static void buildPath(
      boolean failOnOverlap, String fullPath, ProjectionLayer layer, String[] segments) {
    // First create branches
    final int last = segments.length - 1;
    for (int i = 0; i < last; ++i) {
      // Try to find or create branch
      layer = layer.findOrCreateBranch(failOnOverlap, fullPath, segments[i]);
      // May be null if terminal layer found (shorter existing path); if so, we are done
      if (layer == null) {
        return;
      }
    }
    // And then attach terminal (leaf)
    layer.addTerminal(failOnOverlap, fullPath, segments[last]);
  }

  static void buildSlicer(boolean failOnOverlap, SliceDef slice, ProjectionLayer layer) {
    final String fullPath = slice.path;
    String[] segments = DOT.split(fullPath);
    final int last = segments.length - 1;
    for (int i = 0; i < last; ++i) {
      layer = layer.findOrCreateBranch(failOnOverlap, fullPath, segments[i]);
    }
    layer.addSlicer(failOnOverlap, fullPath, segments[last], slice.slicer());
  }

  ProjectionLayer findOrCreateBranch(boolean failOnOverlap, String fullPath, String segment) {
    // Cannot proceed past terminal layer (shorter path):
    if (isTerminal) {
      if (failOnOverlap) {
        reportPathConflict(this.fullPath, fullPath);
      }
      // Otherwise leave node as-is, return null to indicate no further traversal
      return null;
    }
    ProjectionLayer next = nextLayers.get(segment);
    if (next == null) {
      next = new ProjectionLayer(fullPath, false);
      nextLayers.put(segment, next);
    }
    return next;
  }

  void addTerminal(boolean failOnOverlap, String fullPath, String segment) {
    // Cannot proceed past terminal layer (shorter path):
    if (isTerminal) {
      if (failOnOverlap) {
        reportPathConflict(this.fullPath, fullPath);
      }
      // Otherwise leave node as-is, return null to indicate no further traversal
      return;
    }
    // But will also not allow existing longer path:
    if (failOnOverlap) {
      ProjectionLayer next = nextLayers.get(segment);
      if (next != null) {
        reportPathConflict(fullPath, next.fullPath);
      }
    }
    nextLayers.put(segment, new ProjectionLayer(fullPath, true));
  }

  void addSlicer(boolean failOnOverlap, String fullPath, String segment, Slicer slicer) {
    // Similar checks to "regular" paths
    if (isTerminal) {
      if (failOnOverlap) {
        reportPathConflict(this.fullPath, fullPath);
      }
      return;
    }
    if (failOnOverlap) {
      ProjectionLayer next = nextLayers.get(segment);
      if (next != null) {
        reportPathConflict(fullPath, next.fullPath);
      }
    }
    nextLayers.put(segment, new ProjectionLayer(fullPath, slicer));
  }

  /**
   * Method called to check if given path is included in the projection for which this is the root
   * layer: this is done by traversing layers until determination can be made.
   *
   * @param path Dot-notation path to check
   * @return {@code true} if path is included; {@code false} if not.
   */
  public boolean isPathIncluded(String path) {
    final String[] segments = DOT.split(path);
    return isPathIncluded(segments, 0);
  }

  private boolean isPathIncluded(String[] segments, int index) {
    // If we are at a terminal layer, we are done
    if (isTerminal) {
      return true;
    }
    // Otherwise if we are at the end of path, we are not included
    if (index == segments.length) {
      return false;
    }
    // Otherwise we need to traverse further
    ProjectionLayer next = nextLayers.get(segments[index]);
    if (next == null) {
      return false;
    }
    return next.isPathIncluded(segments, index + 1);
  }

  /**
   * Method called to apply Inclusion-based projection, in which everything to be included is
   * enumerated, and the rest need to be removed (this includes document id as well as regular
   * properties).
   *
   * @param subtree Document level to process
   */
  public void applyInclusions(JsonNode subtree) {
    // Arrays are "skipped" in that inclusion only affects Objects
    // (inside Arrays or other Objects)
    if (subtree.isArray()) {
      subtree.forEach(e -> applyInclusions(e));
      return;
    }

    // For Object inclusions we need to traverse existing properties and see if layer
    // has a match; three possibilities:
    // 1. Match, terminal -> include path, value (no-op, skip)
    // 2. Match, non-terminal -> continue checking recursively
    // 3. No match, prune (remove)

    var it = subtree.fields();
    while (it.hasNext()) {
      var entry = it.next();
      ProjectionLayer nextLayer = nextLayers.get(entry.getKey());

      if (nextLayer == null) { // case 3: no match, remove
        it.remove();
      } else if (nextLayer.isTerminal) { // case 1: leave as-is (but "$slice" if need be)
        nextLayer.applySlice(entry.getValue());
      } else { // case 2: recurse
        nextLayer.applyInclusions(entry.getValue());
      }
    }
  }

  /**
   * Method called to apply Exclusion-based projection, in which only things to be removed are
   * enumerated, and the rest of properties are left as-is.
   *
   * @param subtree Document level to process
   */
  public void applyExclusions(JsonNode subtree) {
    // Arrays are "skipped" in that exclusion only affects Objects
    // (inside Arrays or other Objects)
    if (subtree.isArray()) {
      subtree.forEach(e -> applyExclusions(e));
      return;
    }

    // For Object inclusions we can traverse next-layer mappings and see if there
    // is a matching property; this gives us three possibilities:
    // 1. Match, terminal -> remove property
    // 2. Match, non-terminal -> continue checking recursively
    // 3. No match, nothing to do
    var it = nextLayers.entrySet().iterator();
    while (it.hasNext()) {
      var entry = it.next();
      final String propName = entry.getKey();
      ProjectionLayer nextLayer = entry.getValue();
      JsonNode propValue = subtree.get(propName);

      if (propValue == null) { // case 3: no match, leave
        ;
      } else if (nextLayer.isTerminal) { // case 1: remove either partially ("$slice") or completely
        if (!nextLayer.applySlice(propValue)) {
          ((ObjectNode) subtree).remove(propName);
        }
      } else { // case 2: recurse
        nextLayer.applyExclusions(propValue);
      }
    }
  }

  /**
   * Method called on sub-tree on which {@code $slice} operation is to be performed: presumably
   * Array, but not necessarily (if not, will be left as-is).
   *
   * @param subtree JSON value to "slice"
   * @return True if there is "$slice" operation to perform (regardless of whether any change
   *     occurred)
   */
  public boolean applySlice(JsonNode subtree) {
    if (slicer == null) {
      return false;
    }
    slicer.slice(subtree);
    return true;
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

  public record SliceDef(String path, Slicer slicer) {}

  static Slicer constructSlicer(int count) {
    return new SimpleSlicer(count);
  }

  static Slicer constructSlicer(int skip, int count) {
    return new FullSlicer(skip, count);
  }

  interface Slicer {
    void slice(JsonNode arrayNode);
  }

  /**
   * Simple slicer gets just one numeric argument: number of entries to retain; if positive, first
   * N, if negative, last -N. Implemented by removing elements that are not to be retained.
   */
  record SimpleSlicer(int count) implements Slicer {
    @Override
    public void slice(JsonNode n) {
      if (!n.isArray()) {
        return;
      }
      ArrayNode array = (ArrayNode) n;
      int removeAt, toRetain;

      if (count >= 0) { // Retain first N, i.e. remove beyond
        removeAt = toRetain = count;
      } else { // Retain last N, i.e. remove first len-N
        removeAt = 0;
        toRetain = -count;
      }
      while (array.size() > toRetain) {
        array.remove(removeAt);
      }
    }
  }

  /**
   * "Full" slicer gets just two numeric arguments: number of entries to skip first, then number of
   * entries to return. Skip value can be positive or negative; positive skips first N, negative
   * last -N. "toReturn" value must be positive integer.
   *
   * <p>Implemented by removing elements that are not to be retained.
   */
  record FullSlicer(int skip, int toReturn) implements Slicer {
    @Override
    public void slice(JsonNode n) {
      if (!n.isArray()) {
        return;
      }
      ArrayNode array = (ArrayNode) n;
      int firstToRemove;

      if (skip >= 0) { // Skip (remove) first N
        firstToRemove = Math.min(skip, array.size());
      } else { // Retain last N, i.e. remove first len-N
        firstToRemove = Math.max(0, array.size() + skip);
      }

      // So: first remove N head elements
      while (--firstToRemove >= 0) {
        array.remove(0);
      }

      // And then last N tail elements
      while (array.size() > toReturn) {
        array.remove(toReturn);
      }
    }
  }
}
