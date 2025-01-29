package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.InvertibleCommandClause;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    type = SchemaType.OBJECT,
    implementation = Object.class,
    example =
        """
             {"name": "Aaron", "country": "US"}
              """)
public abstract class FilterClause implements InvertibleCommandClause {

  protected final LogicalExpression logicalExpression;

  public FilterClause(LogicalExpression logicalExpression) {
    this.logicalExpression = logicalExpression;
  }

  public LogicalExpression logicalExpression() {
    return logicalExpression;
  }

  public void invertForTableCommand(CommandContext<TableSchemaObject> commandContext) {
    invertLogicalExpression(logicalExpression, null);
  }

  public void invertForCollectionCommand(CommandContext<CollectionSchemaObject> commandContext) {
    invertLogicalExpression(logicalExpression, null);
  }

  /**
   * Recursive method Invert the currentLogicalExpression, add its inverted children to
   * parentLogicalExpression examples: 1. {age=10, not{name=tim}} -> one comparisonExpression within
   * not, just revert it and add to ancestorLogicalExpression -> {age=10, name!=tim}
   *
   * <p>2. {age=10, not{or[address=Shanghai ,gender=male]}} -> one logicalExpression within not,
   * just revert it and add to ancestorLogicalExpression -> {age=10, and[address!=Shanghai
   * ,gender!=male]}
   *
   * <p>3. {age=10, not{name=tim, school=cmu}} -> two comparisonExpressions within not, revert them
   * and construct explicit or relation -> {age=10, or[name!=tim, school!=cmu]}
   *
   * <p>4. {age=10, not{or[address=Shanghai ,gender=male], name=tim}} -> one comparisonExpression
   * and one logicalExpression within not -> invert them and construct explicit or relation ->
   * {age=10, or[and[address!=Shanghai ,gender!=male], name!=tim]}
   *
   * <p>5. {age=10, not{or[address=Shanghai ,gender=male], and[color=yellow,height=175]}} ->
   * multiple logicalExpressions within not -> invert them and construct explicit or relation ->
   * {age=10, or[and[address!=Shanghai ,gender!=male], or[color!=yellow,height!=175]]}
   *
   * @param logicalExpression current logicalExpression
   * @param parentLogicalExpression parent logicalExpression
   */
  private void invertLogicalExpression(
      LogicalExpression logicalExpression, LogicalExpression parentLogicalExpression) {

    // create this temp list to avoid concurrentModification
    List<LogicalExpression> tempLogicalExpressions =
        new ArrayList<>(logicalExpression.logicalExpressions);

    for (LogicalExpression childLogicalExpression : tempLogicalExpressions) {
      invertLogicalExpression(childLogicalExpression, logicalExpression);
    }

    Iterator<LogicalExpression> iterator = logicalExpression.logicalExpressions.iterator();
    while (iterator.hasNext()) {
      LogicalExpression childLogicalExpression = iterator.next();
      if (childLogicalExpression.getLogicalRelation() == LogicalExpression.LogicalOperator.NOT) {
        iterator.remove();
      }
    }

    // Handle all the comparisonExpressions and logicalExpressions inside this $ånot
    if (logicalExpression.getLogicalRelation() == LogicalExpression.LogicalOperator.NOT) {
      // 1. recursively flip all the ComparisonExpression and LogicalExpression
      flip(logicalExpression);

      // 2. Different of situations here
      if (logicalExpression.comparisonExpressions.size() == 1
          && logicalExpression.logicalExpressions.isEmpty()) {
        // 2.1 only one comparisonExpression
        parentLogicalExpression.addComparisonExpressionsFlipped(
            List.of(logicalExpression.comparisonExpressions.get(0)));
      } else if (!logicalExpression.logicalExpressions.isEmpty()
          && logicalExpression.comparisonExpressions.isEmpty()) {
        // 2.2 only one logicalExpression
        logicalExpression.logicalExpressions.forEach(
            parentLogicalExpression::addLogicalExpressionFlipped);
      } else {
        // 2.3 multiple comparisonExpression
        // 2.4 multiple comparisonExpression and multiple logicalExpression
        // 2.5 multiple logicalExpression
        final LogicalExpression or = LogicalExpression.or();
        logicalExpression.comparisonExpressions.forEach(
            comparisonExpression ->
                or.addComparisonExpressionsFlipped(List.of(comparisonExpression)));
        logicalExpression.logicalExpressions.forEach(or::addLogicalExpressionFlipped);

        parentLogicalExpression.addLogicalExpressionFlipped(or);
      }

      // TODO: Is this needed? since we will remove the $not node no matter what
      // 3. clear the all the comparisonExpressions inside this $NOT
      logicalExpression.comparisonExpressions.clear();
    }
  }

  /**
   * Recursive method Invert the currentLogicalExpression recursively 1. AND -> OR 2. OR -> AND
   * 3.comparisonExpression -> opposite comparisonExpression
   *
   * @param logicalExpression logicalExpression
   */
  private void flip(LogicalExpression logicalExpression) {
    logicalExpression.setLogicalRelation(
        (LogicalExpression.LogicalOperator) logicalExpression.getLogicalRelation().invert());
    // flip child LogicalExpressions
    for (LogicalExpression childLogicalExpression : logicalExpression.logicalExpressions) {
      flip(childLogicalExpression);
    }
    // flip child ComparisonExpression
    for (ComparisonExpression childComparisonExpression : logicalExpression.comparisonExpressions) {
      childComparisonExpression.invert();
    }
  }
}
