package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.querybuilder.update.OngoingAssignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import java.util.List;
import java.util.Objects;

/**
 * Default implementation of a {@link UpdateValuesCQLClause} that applies a list of {@link ColumnAssignment}s.
 * <p>
 * This class exists for two reasons: want to keep all the {@link CQLClause} immediate subclasses as interfaces,
 * and when we move Collections to the Java driver query builder it may have a more complex implementation.
 */
public class DefaultUpdateValuesCQLClause implements UpdateValuesCQLClause {

  private final List<ColumnAssignment> assignments;

  public DefaultUpdateValuesCQLClause(List<ColumnAssignment> assignments) {
    this.assignments = Objects.requireNonNull(assignments, "assignments must not be null");
    if (assignments.isEmpty()) {
      throw new IllegalArgumentException("assignments must not be empty");
    }
  }

  @Override
  public UpdateWithAssignments apply(OngoingAssignment ongoingAssignment, List<Object> objects) {

    UpdateWithAssignments updateWithAssignments = null;
    for (ColumnAssignment assignment : assignments) {
      updateWithAssignments =
          (updateWithAssignments == null)
              ? assignment.apply(ongoingAssignment, objects)
              : assignment.apply(updateWithAssignments, objects);
    }
    return updateWithAssignments;
  }
}
