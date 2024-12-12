package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import io.stargate.sgv2.jsonapi.api.model.command.Projectable;
import io.stargate.sgv2.jsonapi.api.model.command.VectorSortable;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiSupportDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.util.CqlVectorUtil;
import java.util.function.Function;

public interface TableSimilarityFunction extends Function<Select, Select> {

  TableSimilarityFunction NO_OP = new TableSimilarityFunctionNoOp();

  // Make a unique constant string as similarity score function alias in cql statement
  // E.G. SELECT id,similarity_euclidean(vector_type,[0.2, 0.15, 0.3]) AS
  // similarityScore1699123456789 from xxx;
  String SIMILARITY_SCORE_ALIAS = "similarityScore" + System.currentTimeMillis();

  static <CmdT extends Projectable> TableSimilarityFunction from(
      CmdT command, TableSchemaObject table) {

    if (!(command instanceof VectorSortable)) {
      return NO_OP;
    }
    var vectorSortable = (VectorSortable) command;

    var sortExpressionOptional = vectorSortable.vectorSortExpression();
    if (sortExpressionOptional.isEmpty()) {
      // nothing to sort on, so nothing to return even if they asked for the similarity score
      return NO_OP;
    }
    var sortExpression = sortExpressionOptional.get();

    var includeSimilarityScore = vectorSortable.includeSimilarityScore().orElse(false);
    if (!includeSimilarityScore) {
      // user does not ask for similarityScore
      return NO_OP;
    }

    var requestedVectorColumnPath = sortExpression.pathAsCqlIdentifier();
    // TODO: This is a hack, we need to refactor these methods in ApiColumnDefContainer.
    // Currently, this matcher is just for match vector columns, and then to avoid hit the
    // typeName() placeholder exception in UnsupportedApiDataType
    var matcher =
        ApiSupportDef.Matcher.NO_MATCHES.withCreateTable(true).withInsert(true).withRead(true);
    var apiColumnDef =
        table
            .apiTableDef()
            .allColumns()
            .filterBySupport(matcher)
            .filterByApiTypeName(ApiTypeName.VECTOR)
            .get(requestedVectorColumnPath);
    if (apiColumnDef == null) {
      // column does not exist or is not a vector, ignore because sort will fail
      return NO_OP;
    }

    var vectorColDefinition = table.vectorConfig().getColumnDefinition(requestedVectorColumnPath);
    if (vectorColDefinition.isEmpty()) {
      // no requested vector column on the table
      return NO_OP;
    }

    // similarityFunction is from index, default to cosine. In projection,
    // we do not care about if the vector column in indexed or not, capture by vector sort.
    var similarityFunction = vectorColDefinition.get().similarityFunction().cqlProjectionFunction();

    return new TableSimilarityFunctionImpl(
        requestedVectorColumnPath,
        CqlVectorUtil.floatsToCqlVector(sortExpression.vector()),
        similarityFunction);
  }

  boolean canProjectSimilarity();

  class TableSimilarityFunctionImpl implements TableSimilarityFunction {
    private final CqlIdentifier requestedVectorColumnPath;
    private final CqlVector<Float> vector;
    private final String function;

    public TableSimilarityFunctionImpl(
        CqlIdentifier requestedVectorColumnPath, CqlVector<Float> vector, String function) {
      this.requestedVectorColumnPath = requestedVectorColumnPath;
      this.vector = vector;
      this.function = function;
    }

    @Override
    public Select apply(Select select) {
      return select
          .function(function, Selector.column(requestedVectorColumnPath), literal(vector))
          .as(SIMILARITY_SCORE_ALIAS);
    }

    @Override
    public boolean canProjectSimilarity() {
      return true;
    }
  }

  public class TableSimilarityFunctionNoOp implements TableSimilarityFunction {
    @Override
    public Select apply(Select select) {
      return select;
    }

    @Override
    public boolean canProjectSimilarity() {
      return false;
    }
  }
}
