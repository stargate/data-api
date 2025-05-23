package io.stargate.sgv2.jsonapi.service.cqldriver.override;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.CqlSnippet;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is to add AND/OR ability to driver where clauses, and it is a temporary override
 * solution, should be removed once the driver supports it natively. Note, although AND/OR has been
 * implemented in DataStax Astra, it is not yet in Apache Cassandra. So the community Java driver
 * does not support this as of now. See <a
 * href="https://issues.apache.org/jira/browse/CASSJAVA-47">Ticket CASSJAVA-47</a>.
 */
public class DefaultSubConditionRelation
    implements OngoingWhereClause<DefaultSubConditionRelation>, BuildableQuery, Relation {

  private final List<Relation> relations;
  private final boolean isSubCondition;

  /** Construct sub-condition relation with empty WHERE clause. */
  public DefaultSubConditionRelation(boolean isSubCondition) {
    this.relations = new ArrayList<>();
    this.isSubCondition = isSubCondition;
  }

  @NonNull
  @Override
  public DefaultSubConditionRelation where(@NonNull Relation relation) {
    relations.add(relation);
    return this;
  }

  @NonNull
  @Override
  public DefaultSubConditionRelation where(@NonNull Iterable<Relation> additionalRelations) {
    for (Relation relation : additionalRelations) {
      relations.add(relation);
    }
    return this;
  }

  @NonNull
  public DefaultSubConditionRelation withRelations(@NonNull List<Relation> newRelations) {
    relations.addAll(newRelations);
    return this;
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder();

    if (isSubCondition) {
      builder.append("(");
    }
    appendWhereClause(builder, relations, isSubCondition);
    if (isSubCondition) {
      builder.append(")");
    }

    return builder.toString();
  }

  public static void appendWhereClause(
      StringBuilder builder, List<Relation> relations, boolean isSubCondition) {
    boolean first = true;
    for (int i = 0; i < relations.size(); ++i) {
      CqlSnippet snippet = relations.get(i);
      if (first && !isSubCondition) {
        builder.append(" WHERE ");
      }
      first = false;

      snippet.appendTo(builder);

      boolean logicalOperatorAdded = false;
      LogicalRelation logicalRelation = lookAheadNextRelation(relations, i, LogicalRelation.class);
      if (logicalRelation != null) {
        builder.append(" ");
        logicalRelation.appendTo(builder);
        builder.append(" ");
        logicalOperatorAdded = true;
        ++i;
      }
      if (!logicalOperatorAdded && i + 1 < relations.size()) {
        builder.append(" AND ");
      }
    }
  }

  private static <T extends Relation> T lookAheadNextRelation(
      List<Relation> relations, int position, Class<T> clazz) {
    if (position + 1 >= relations.size()) {
      return null;
    }
    Relation relation = relations.get(position + 1);
    if (relation.getClass().isAssignableFrom(clazz)) {
      return (T) relation;
    }
    return null;
  }

  @NonNull
  @Override
  public SimpleStatement build() {
    return builder().build();
  }

  @NonNull
  @Override
  public SimpleStatement build(@NonNull Object... values) {
    return builder().addPositionalValues(values).build();
  }

  @NonNull
  @Override
  public SimpleStatement build(@NonNull Map<String, Object> namedValues) {
    SimpleStatementBuilder builder = builder();
    for (Map.Entry<String, Object> entry : namedValues.entrySet()) {
      builder.addNamedValue(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return asCql();
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    builder.append(asCql());
  }

  @Override
  public boolean isIdempotent() {
    for (Relation relation : relations) {
      if (!relation.isIdempotent()) {
        return false;
      }
    }
    return true;
  }

  /** Adds conjunction clause. Next relation is logically joined with AND. */
  public OngoingWhereClause<DefaultSubConditionRelation> and() {
    return where(LogicalRelation.AND);
  }

  /** Adds alternative clause. Next relation is logically joined with OR. */
  @NonNull
  @CheckReturnValue
  public OngoingWhereClause<DefaultSubConditionRelation> or() {
    return where(LogicalRelation.OR);
  }

  /** Creates new sub-condition in the WHERE clause, surrounded by parenthesis. */
  @NonNull
  public static DefaultSubConditionRelation subCondition() {
    return new DefaultSubConditionRelation(true);
  }
}
