package io.stargate.sgv2.jsonapi.service.operation.query.extendedDriverQuerybuilder;

import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

@Immutable
public class LogicalRelation implements Relation {
  public static final LogicalRelation AND = new LogicalRelation("AND");
  public static final LogicalRelation OR = new LogicalRelation("OR");

  private final String operator;

  public LogicalRelation(@NonNull String operator) {
    Preconditions.checkNotNull(operator);
    this.operator = operator;
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    builder.append(operator);
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }
}
