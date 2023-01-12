package io.stargate.sgv3.docsapi.service.resolver.model.impl.matcher;

import io.stargate.sgv3.docsapi.api.model.command.Command;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.FilterOperation;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.FilterOperator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The per API request result of running a {@link FilterMatcher.Capture} against a {@link Command}.
 *
 * <p>Is identified by the same Marker as the capture, collects the values the capture matched with
 * and provides a way for the resolver to pull them out to use in commands.
 *
 * <p>Values from all of the operations that matched, they will be multiple values when we start
 * using greedy matching e.g. match all the string comparison operations in {"username": "foo",
 * "address.street": "bar"} Path in the {@link ComparisonExpression} that was matched, e.g.
 * "address.street"
 *
 * <p>Created by the {@link CaptureGroups} via a builder
 */
public record CaptureGroup<TYPE>(Map<String, List<FilterOperation<TYPE>>> captures) {

  void consumeAllCaptures(Consumer<CapturePair<TYPE>> consumer) {
    captures.forEach(
        (key, operations) -> {
          operations.forEach(
              operation ->
                  consumer.accept(
                      new CapturePair<TYPE>(
                          key, operation.operator(), operation.operand().value())));
        });
  }

  public void withCapture(String path, List<FilterOperation<TYPE>> capture) {
    captures.put(path, capture);
  }

  /**
   * Here so we have simple consumer for consumeCaptures.
   *
   * <p>May also need to expand this to include the operation.
   */
  public static record CapturePair<TYPE>(String path, FilterOperator operator, TYPE value) {}
}
