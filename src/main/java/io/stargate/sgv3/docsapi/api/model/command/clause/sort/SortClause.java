package io.stargate.sgv3.docsapi.api.model.command.clause.sort;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv3.docsapi.api.model.command.deserializers.SortClauseDeserializer;
import java.util.List;
import javax.validation.Valid;

/**
 * Internal model for the sort clause that can be used in the commands.
 *
 * @param sortExpressions Ordered list of sort expressions.
 */
@JsonDeserialize(using = SortClauseDeserializer.class)
public record SortClause(@Valid List<SortExpression> sortExpressions) {}
