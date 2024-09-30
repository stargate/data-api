package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilterAnalyzedUsage;
import java.util.ArrayList;
import java.util.List;

/**
 * The purpose for this analyzer class is to gather all the analyze result {@link
 * TableFilterAnalyzedUsage} from all {@link TableFilter} in the TableWhereCQLClause. And then
 * proceed with overall analyze.
 *
 * <p>E.G. {"filter": {"name": "Lisa", "age":25}}. <br>
 * This API filter will resolve as two TableFilters against a table. And since name column is not
 * indexed, the corresponding tableFilter will result a {@link TableFilterAnalyzedUsage} which
 * contains information like "ALLOW FILTERING" and "warning msg". Then after
 * tableWhereCQLClauseAnalyzer gathered all the {@link TableFilterAnalyzedUsage}, it will make final
 * decision of analyzing the TableWhereCQLClause.
 *
 * <p>TODO, this class will be helpful for future features, such as TableFilter and Index
 * recommendation.
 */

// TODO, the naming, actually it is just for table. should it be called as
// TableWhereCQLClauseAnalyzer
public class WhereCQLClauseAnalyzer {

  private final DBLogicalExpression dbLogicalExpression;
  private final TableSchemaObject tableSchemaObject;

  public WhereCQLClauseAnalyzer(
      DBLogicalExpression dbLogicalExpression, TableSchemaObject tableSchemaObject) {
    this.dbLogicalExpression = dbLogicalExpression;
    this.tableSchemaObject = tableSchemaObject;
  }

  /**
   * Analyse the all DB filter usage of TableWhereCQLClause.<br>
   * Iterate very dbFilter in dbLogicalExpression, get the list of DBFilterUsage.
   */
  public WhereCQLClauseAnalyzeResult analyse() {

    // Step 1: Analyze individual TableFilter first, get all TableFilterAnalyzedUsage.
    List<TableFilterAnalyzedUsage> tableFilterAnalyzedUsages = new ArrayList<>();
    analyseUsageOfTableFilters(dbLogicalExpression, tableFilterAnalyzedUsages);

    // Step 2: Make decision.
    boolean withAllowFiltering = false;
    List<String> warnings = new ArrayList<>();
    for (TableFilterAnalyzedUsage tableFilterAnalyzedUsage : tableFilterAnalyzedUsages) {
      withAllowFiltering |= tableFilterAnalyzedUsage.allowFiltering();
      tableFilterAnalyzedUsage.warning().ifPresent(warnings::add);
    }

    return new WhereCQLClauseAnalyzeResult(withAllowFiltering, warnings);
  }

  /**
   * The private helper method to recursively traverse the DBLogicalExpression. <br>
   * During the traverse, it will populate the dbFilterUsages list by analyse each DBFilter.
   *
   * @param dbLogicalExpression Logical relation container of DBFilters
   * @param dbFilterUsages List of analyse result for DBFilters
   */
  private void analyseUsageOfTableFilters(
      DBLogicalExpression dbLogicalExpression, List<TableFilterAnalyzedUsage> dbFilterUsages) {

    // iterate all dBFilters at current level of DBLogicalExpression
    for (DBFilterBase dbFilterBase : dbLogicalExpression.dBFilters()) {
      TableFilter tableFilter = (TableFilter) dbFilterBase;
      dbFilterUsages.add(tableFilter.analyze(tableSchemaObject));
    }
    // iterate sub dBLogicalExpression
    for (DBLogicalExpression subDBlogicalExpression : dbLogicalExpression.dbLogicalExpressions()) {
      analyseUsageOfTableFilters(subDBlogicalExpression, dbFilterUsages);
    }
  }

  public record WhereCQLClauseAnalyzeResult(boolean withAllowFiltering, List<String> warnings
      //        public List<String> suggestions;
      ) {}
  ;
}
