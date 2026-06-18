package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import java.util.List;

public class WhereCQLClauseTestData extends TestDataSuplier {

  public WhereCQLClauseTestData(TestData testData) {
    super(testData);
  }

  public WhereCQLClause<Select> emptySelect() {
    return new WhereCQLClause<>() {
      @Override
      public Select apply(Select select, List<Object> objects) {
        return select;
      }

      @Override
      public DBLogicalExpression getLogicalExpression() {
        return null;
      }

      @Override
      public boolean selectsSinglePartition(TableSchemaObject tableSchemaObject) {
        throw new IllegalArgumentException(
            "WhereCQLClauseTestData does not support partitionKeysFullyRestrictedByEq");
      }
    };
  }
}
