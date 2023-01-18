package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv3.docsapi.api.model.command.deserializers.FilterClauseDeserializer;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonDeserialize(using = FilterClauseDeserializer.class)
@Schema(
    type = SchemaType.OBJECT,
    implementation = Object.class,
    example = """
              {"col1": "val1", "col2", "val"}
              """)
public record FilterClause(List<ComparisonExpression> comparisonExpressions) {}
