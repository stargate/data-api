package io.stargate.sgv3.docsapi.api.model.command.clause.sort;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv3.docsapi.api.model.command.deserializers.SortClauseDeserializer;
import java.util.List;
import javax.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Internal model for the sort clause that can be used in the commands.
 *
 * @param sortExpressions Ordered list of sort expressions.
 */
@JsonDeserialize(using = SortClauseDeserializer.class)
@Schema(
    type = SchemaType.ARRAY,
    implementation = String[].class,
    example = """
              ["-user.age", "user.name"]
              """)
public record SortClause(@Valid List<SortExpression> sortExpressions) {}
