package io.stargate.sgv2.jsonapi.service.cqldriver.override;

import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.NonNull;

/**
 * This class is to add AND/OR relation support to java driver, and it is a temporary override
 * solution, should be removed once the driver supports it natively. Note, although AND/OR has been
 * implemented in DataStax Astra, it is not yet in Apache Cassandra. So the community Java driver
 * does not support this as of now. See <a
 * href="https://issues.apache.org/jira/browse/CASSJAVA-47">Ticket CASSJAVA-47</a>.
 */
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
