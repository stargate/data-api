package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterLogicalExpression;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 *
 * <p>T - The {@link Command} that is filtered against
 *
 * <p>Since we need to keep the logical relation information when doing captures for
 * LogicalExpression, so CaptureGroups needs to have a recursive defined structure
 */
public class CaptureGroups<T extends Command> {

  private DBFilterLogicalExpression.DBLogicalOperator dbLogicalOperator;
  private final T command;

  public final Map<Object, CaptureGroup<?>> captureGroupMap;

  private final List<CaptureGroups<T>> captureGroupsList;

  public CaptureGroups(T command, DBFilterLogicalExpression.DBLogicalOperator dbLogicalOperator) {
    this.command = command;
    this.dbLogicalOperator = dbLogicalOperator;
    this.captureGroupMap = new HashMap<>();
    this.captureGroupsList = new ArrayList<>();
  }

  /**
   * Get the {@link CaptureGroup} that has the result of the {@link FilterMatcher.Capture} created
   * with the supplied marker, construct one if absent
   *
   * @param marker
   * @return CaptureGroup
   */
  public CaptureGroup<?> getGroup(Object marker) {
    return captureGroupMap.computeIfAbsent(marker, f -> new CaptureGroup(new HashMap<>()));
  }

  /**
   * Get the {@link CaptureGroup} that has the result of the {@link FilterMatcher.Capture} created
   * with the supplied marker
   *
   * @param marker
   * @return CaptureGroup
   */
  public CaptureGroup<?> getGroupIfPresent(Object marker) {
    return captureGroupMap.get(marker);
  }

  /**
   * getter method for the DBLogicalOperator
   *
   * @return DBFilterLogicalExpression.DBLogicalOperator
   */
  public DBFilterLogicalExpression.DBLogicalOperator getLogicalOperator() {
    return dbLogicalOperator;
  }

  /**
   * add next logical relation level of captureGroups to the captureGroupsList this method will be
   * called when FilterMatcher convert LogicalExpression and populate the captureGroups
   *
   * @param captureGroups captureGroups
   * @return captureGroups
   */
  public CaptureGroups<T> addCaptureGroups(CaptureGroups<T> captureGroups) {
    captureGroupsList.add(captureGroups);
    return captureGroups;
  }

  /**
   * The recursive method to help consume captureGroup and convert captures to DBFilters. This
   * method will be called by resolver method in {@link CollectionFilterResolver} {@link
   * TableFilterResolver} Resolver method defines the consumer of how to resolve CaptureGroup to
   * DBFilter. This recursive method will help to consume in a recursive way and populate the
   * DBFilterLogicalExpression
   *
   * @param currentDbFilterLogicalExpression, the current DBFilterLogicalExpression to be populated
   * @param consumer, consumer defines how to consume the current CaptureGroups and current
   *     DBFilterLogicalExpression
   */
  public void recursiveConsume(
      DBFilterLogicalExpression currentDbFilterLogicalExpression,
      BiConsumer<CaptureGroups, DBFilterLogicalExpression> consumer) {

    final List<CaptureGroups<T>> captureGroupsList = this.captureGroupsList;
    for (CaptureGroups<T> innerCaptureGroups : captureGroupsList) {
      DBFilterLogicalExpression innerDBFilterLogicalExpression =
          currentDbFilterLogicalExpression.addDBFilterLogicalExpression(
              new DBFilterLogicalExpression(innerCaptureGroups.getLogicalOperator()));
      innerCaptureGroups.recursiveConsume(innerDBFilterLogicalExpression, consumer);
    }

    consumer.accept(this, currentDbFilterLogicalExpression);
  }
}
