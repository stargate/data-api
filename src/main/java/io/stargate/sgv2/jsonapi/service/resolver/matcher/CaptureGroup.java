package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperator;
import java.util.ArrayList;
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
 * <p>Created by the {@link CaptureGroups} via a builder <DataType> This is Data type of the object,
 * type for the captureExpression value
 */
public record CaptureGroup<DataType>(Map<String, List<FilterOperation<DataType>>> captures) {

  /**
   * consumer all the captures for this captureGroup it takes the consumer object which defines how
   * to construct corresponding dbFilter{@link CollectionFilterResolver} {@link TableFilterResolver}
   *
   * @param consumer
   */
  void consumeAllCaptures(Consumer<CaptureExpression<DataType>> consumer) {
    captures.forEach(
        (key, operations) -> {
          operations.forEach(
              operation ->
                  consumer.accept(
                      new CaptureExpression<DataType>(
                          key, operation.operator(), operation.operand().value())));
        });
  }

  /**
   * update capture entry within the captureGroup for value update, need to merge captures into
   * single list This is to handle multiple filters with same path within logical operator e.g. if
   * no merge, then value overrides will happen "$or": [ { "username": "user1" }, { "username":
   * "user2" } ]
   *
   * @param path
   * @param capture
   */
  public void withCapture(String path, List<FilterOperation<DataType>> capture) {
    captures.computeIfAbsent(path, k -> new ArrayList<>()).addAll(capture);
  }

  /**
   * Here so we have simple consumer for consumeCaptures.
   *
   * <p>May also need to expand this to include the operation.
   */
  public static record CaptureExpression<DataType>(
      String path, FilterOperator operator, DataType value) {}
}