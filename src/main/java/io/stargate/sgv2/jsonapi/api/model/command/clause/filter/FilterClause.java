package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    type = SchemaType.OBJECT,
    implementation = Object.class,
    example =
        """
             {"name": "Aaron", "country": "US"}
              """)
public abstract class FilterClause {
  protected final LogicalExpression logicalExpression;

  public FilterClause(LogicalExpression logicalExpression) {
    this.logicalExpression = logicalExpression;
  }

  public LogicalExpression logicalExpression() {
    return logicalExpression;
  }
}
