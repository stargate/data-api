package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import java.util.*;
import java.util.function.BiConsumer;

/**
 * All the {@link CaptureGroup}s we got from matching against a command.
 *
 * <p>This is "result" of running the FilterMatcher, and the value we pass to the resolver function
 * so it has raw command and all the groups.
 *
 * <p>Each Capture you create from {@link FilterMatcher#capture(Object)} with a Marker is available
 * here as a {@link CaptureGroup} via {@link #getGroup(Object)}.
 *
 * <p>Created in the {@link FilterMatcher} via a builder
 */
public class CaptureGroups<T extends Command> {

  private final DBLogicalExpression.DBLogicalOperator dbLogicalOperator;

  public final Map<Object, CaptureGroup<?>> captureGroupByMarker = new HashMap<>();

  private final List<CaptureGroups<T>> subCaptureGroups = new ArrayList<>();

  public CaptureGroups(DBLogicalExpression.DBLogicalOperator dbLogicalOperator) {
    this.dbLogicalOperator = dbLogicalOperator;
  }

  /**
   * Get the {@link CaptureGroup} that has the result of the {@link FilterMatcher.Capture} created
   * with the supplied marker, construct one if absent
   *
   * @param marker
   * @return CaptureGroup
   */
  public CaptureGroup<?> getGroup(Object marker) {
    return captureGroupByMarker.computeIfAbsent(marker, f -> new CaptureGroup());
  }

  /**
   * Get the {@link CaptureGroup} that has the result of the {@link FilterMatcher.Capture} created
   * with the supplied marker. If there is no target marker in captureGroup map, return an empty
   * optional
   *
   * @param marker
   * @return Optional<CaptureGroup>
   */
  public Optional<CaptureGroup<?>> getGroupIfPresent(Object marker) {
    return Optional.ofNullable(captureGroupByMarker.get(marker));
  }

  public DBLogicalExpression.DBLogicalOperator getLogicalOperator() {
    return dbLogicalOperator;
  }

  /**
   * Add sub captureGroups to the subCaptureGroups. this method will be called when FilterMatcher
   * convert LogicalExpression and populate the captureGroups
   *
   * @param captureGroups captureGroups
   * @return captureGroups
   */
  public CaptureGroups<T> addSubCaptureGroups(CaptureGroups<T> captureGroups) {
    subCaptureGroups.add(captureGroups);
    return captureGroups;
  }

  /**
   * The recursive method to help consume captureGroup and convert captures to DBFilters. This
   * method will be called by resolver method in {@link CollectionFilterResolver} {@link
   * TableFilterResolver} Resolver method defines the consumer of how to resolve CaptureGroup to
   * DBFilter. This recursive method will help to consume in a recursive way and populate the
   * DBLogicalExpression
   *
   * <p>e.g. { "name":"Jack","$or":[{"age":35},{"city":"LA"}] } first call of recursiveConsume A:
   * currentDbLogicalExpression is an implicit and caller CaptureGroups has {"name":"Jack"} captured
   * in the captureGroupMap, and "$or":[{"age":35},{"city":"LA"}] captured in captureGroupsList.
   *
   * <p>next call of recursiveConsume B: B will be called from A as populating
   * innerdbLogicalExpression. and {"age":35},{"city":"LA"} are captured in the captureGroupMap of
   * innerCaptureGroups
   *
   * <p>note the order of dbFilters in the result DBLogicalExpression only depends on how we consume
   * captures in filterResolver. And since they are in the same logical relation context, so order
   * does not matter regarding how DB query work.
   *
   * @param BiConsumer The BiConsumer takes two parameters, captureGroups and dBLogicalExpression.
   *     it defines * how to consume the current CaptureGroups and current DBLogicalExpression. It
   *     will be * constructed in FilterResolver, the behavior is to get CaptureGroup from
   *     captureGroupMap, convert to corresponding dbFilter and add to dbLogicalExpression
   * @param currentDbLogicalExpression, the current DBLogicalExpression to be populated
   * @param consumer, consumer defines how to consume the current CaptureGroups and current
   *     DBLogicalExpression.
   */
  public void consumeAll(
      DBLogicalExpression currentDbLogicalExpression,
      BiConsumer<CaptureGroups, DBLogicalExpression> consumer) {

    for (CaptureGroups<T> captureGroups : subCaptureGroups) {
      // need to create a logical expression for each sub CaptureGroups, the CaptureGroups
      // represents the logical
      // relation in the API query, so we need to create a logical expression for each CaptureGroups
      DBLogicalExpression subDBLogicalExpression =
          currentDbLogicalExpression.addSubExpression(
              new DBLogicalExpression(captureGroups.getLogicalOperator()));
      captureGroups.consumeAll(subDBLogicalExpression, consumer);
    }

    consumer.accept(this, currentDbLogicalExpression);
  }
}
