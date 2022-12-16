package io.stargate.sgv3.docsapi.commands.resolvers;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.clauses.filter.JsonLiteral;
import io.stargate.sgv3.docsapi.commands.clauses.filter.JsonLiteralOrList;
import io.stargate.sgv3.docsapi.commands.resolvers.FilterMatcher.Capture;
import io.stargate.sgv3.docsapi.shredding.JSONPath;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The per API request result of running a {@link Capture} against a {@link Command}.
 *
 * <p>Is identified by the same Marker as the capture, collects the values the capture matched with
 * and provides a way for the resolver to pull them out to use in commands.
 *
 * <p>Created by the {@link CaptureGroups} via a builder
 */
public class CaptureGroup {
  // Same marker object used to create the Capture
  private final Object marker;

  // Values from all of the operations that matched, they will be multiple values when we start
  // using greedy matching
  // e.g. match all the string comparison operations in {"username": "foo", "address.street": "bar"}
  // Path in the {@link ComparisonExpression} that was matched, e.g. "address.street"
  private final Map<JSONPath, JsonLiteralOrList> values;

  CaptureGroup(Object marker, Map<JSONPath, JsonLiteralOrList> captures) {
    this.marker = marker;
    this.values = captures;
  }

  public Object getMarker() {
    return marker;
  }

  /**
   * Get a single JSON literal and its path, checks that is all we have
   *
   * <p>Would add more for iterating etc.
   *
   * @return
   */
  CapturePairLiteral getSingleLiteral() {
    if (values.size() != 1) {
      throw new RuntimeException(String.format("captures size != 1, is %s", values.size()));
    }
    return values.entrySet().stream()
        .findFirst()
        .map(entry -> new CapturePairLiteral(entry.getKey(), entry.getValue().safeGetLiteral()))
        .get();
  }

  void consumeAllCaptures(Consumer<CapturePair> consumer) {
    values.forEach((key, value) -> consumer.accept(new CapturePair(key, value)));
  }

  protected static Builder builder(Object marker) {
    return new Builder(marker);
  }

  protected static class Builder {
    public final Object marker;
    private final Map<JSONPath, JsonLiteralOrList> captures = new HashMap<>();

    private Builder(Object marker) {
      this.marker = marker;
    }

    public Builder withCapture(JSONPath path, JsonLiteralOrList capture) {
      captures.put(path, capture);
      return this;
    }

    CaptureGroup build() {
      Preconditions.checkState(
          captures.size() > 0, "Must be at least 1 capture, got %s", captures.size());

      return new CaptureGroup(marker, ImmutableMap.copyOf(captures));
    }
  }

  /**
   * Here so we have simple consumer for consumeCaptures.
   *
   * <p>May also need to expand this to include the operation.
   */
  public static class CapturePair {
    public final JSONPath path;
    public final JsonLiteralOrList literalOrList;

    public CapturePair(JSONPath path, JsonLiteralOrList literalOrList) {
      this.path = path;
      this.literalOrList = literalOrList;
    }
  }

  public static class CapturePairLiteral {
    public final JSONPath path;
    public final JsonLiteral literal;

    protected CapturePairLiteral(JSONPath path, JsonLiteral literal) {
      this.path = path;
      this.literal = literal;
    }
  }
}
