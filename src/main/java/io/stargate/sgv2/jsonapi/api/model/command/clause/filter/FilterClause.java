package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import java.util.Objects;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    type = SchemaType.OBJECT,
    implementation = Object.class,
    example =
        """
             {"name": "Aaron", "country": "US"}
              """)
public class FilterClause {
  protected final LogicalExpression logicalExpression;

  public FilterClause(LogicalExpression logicalExpression) {
    this.logicalExpression = Objects.requireNonNull(logicalExpression);
  }

  /**
   * Same as:
   *
   * <pre>
   *   size() == 0
   * </pre>
   */
  public boolean isEmpty() {
    return logicalExpression.isEmpty();
  }

  /**
   * Short-cut to
   *
   * <pre>
   *     logicalExpression.getTotalComparisonExpressionCount()
   * </pre>
   *
   * @return
   */
  public int size() {
    return logicalExpression.getTotalComparisonExpressionCount();
  }

  public LogicalExpression logicalExpression() {
    return logicalExpression;
  }
}
