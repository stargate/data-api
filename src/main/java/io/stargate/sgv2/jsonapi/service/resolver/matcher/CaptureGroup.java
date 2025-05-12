package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperator;
import io.stargate.sgv2.jsonapi.api.model.command.table.MapSetListComponent;
import java.util.ArrayList;
import java.util.HashMap;
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
 *
 * @param <DataType> Data type of the FilterOperation operand JsonLiteral value, the value should be
 *     the Java object value extracted from the Jackson node. type for the captureExpression value
 */
public class CaptureGroup<DataType> {

  /**
   * Key is filter path Value is list of corresponding FilterOperations, each has operator and
   * operand
   */
  private final Map<String, List<FilterOperation<DataType>>> captures = new HashMap<>();

  public CaptureGroup() {}

  /**
   * This method is to consume all the captures for current captureGroup. It takes the consumer
   * object which will construct corresponding dbFilter. See {@link CollectionFilterResolver} or
   * {@link TableFilterResolver} for usage.
   */
  void consumeAllCaptures(Consumer<CaptureExpression<DataType>> consumer) {
    captures.forEach(
        (key, operations) -> {
          operations.forEach(
              operation ->
                  consumer.accept(
                      new CaptureExpression<>(
                          key,
                          operation.operator(),
                          operation.operand().value(),
                          operation.mapSetListComponent())));
        });
  }

  /**
   * Update capture entry within the captureGroup for value update, need to merge captures into
   * single list This is to handle multiple filters with same path within logical operator e.g. If
   * no merge, then value overrides will happen "$or": [ { "username": "user1" }, { "username":
   * "user2" } ]
   *
   * @param path
   * @param capture
   */
  public void addCapture(String path, List<FilterOperation<DataType>> capture) {
    captures.computeIfAbsent(path, k -> new ArrayList<>()).addAll(capture);
  }

  /**
   * Here so we have simple consumer for consumeCaptures.
   *
   * <p>May also need to expand this to include the operation.
   */
  public record CaptureExpression<DataType>(
      String path,
      FilterOperator operator,
      DataType value,
      MapSetListComponent mapSetListComponent) {}
}
