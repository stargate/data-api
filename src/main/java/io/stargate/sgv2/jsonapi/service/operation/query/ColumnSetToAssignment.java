package io.stargate.sgv2.jsonapi.service.operation.query;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.OngoingAssignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import io.stargate.sgv2.jsonapi.service.resolver.update.TableUpdateSetResolver;
import io.stargate.sgv2.jsonapi.service.resolver.update.TableUpdateUnsetResolver;
import java.util.function.BiFunction;

/**
 * Converts a column set operation to an assignment.
 *
 * <p>This is a {@link BiFunction} that takes an {@link OngoingAssignment} and a {@link
 * CqlIdentifier} and returns an {@link UpdateWithAssignments} that represents to set/unset of the
 * column value.
 *
 * <p>Currently resolved from by API operator $set, $unset, see {@link TableUpdateSetResolver},
 * {@link TableUpdateUnsetResolver}.
 */
public class ColumnSetToAssignment
    implements BiFunction<OngoingAssignment, CqlIdentifier, UpdateWithAssignments> {

  @Override
  public UpdateWithAssignments apply(OngoingAssignment ongoingAssignment, CqlIdentifier column) {
    return ongoingAssignment.set(Assignment.setColumn(column, bindMarker()));
  }
}
