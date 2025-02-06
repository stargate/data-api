package io.stargate.sgv2.jsonapi.fixtures.testdata;

import io.stargate.sgv2.jsonapi.service.operation.OperationAttemptTestData;
import io.stargate.sgv2.jsonapi.service.operation.ReadDBTaskTestData;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class TestData {

  public final TestDataNames names;

  private final Map<Class<?>, Object> cache = new HashMap<>();

  public TestData() {
    this(new TestDataNames());
  }

  protected TestData(TestDataNames names) {
    this.names = names;
  }

  @SuppressWarnings("unchecked")
  private <T> T getOrCache(Class<T> clazz) {
    return (T)
        cache.computeIfAbsent(
            clazz,
            k -> {
              try {
                Constructor<?> constructor = clazz.getConstructor(TestData.class);
                return constructor.newInstance(this);
              } catch (NoSuchMethodException
                  | InstantiationException
                  | IllegalAccessException
                  | InvocationTargetException e) {
                throw new RuntimeException("Error building TestData class: " + clazz.getName(), e);
              }
            });
  }

  public SchemaObjectTestData schemaObject() {
    return getOrCache(SchemaObjectTestData.class);
  }

  public TableMetadataTestData tableMetadata() {
    return getOrCache(TableMetadataTestData.class);
  }

  public OperationAttemptTestData operationAttempt() {
    return getOrCache(OperationAttemptTestData.class);
  }

  public ReadDBTaskTestData readAttempt() {
    return getOrCache(ReadDBTaskTestData.class);
  }

  public ResultSetTestData resultSet() {
    return getOrCache(ResultSetTestData.class);
  }

  public LogicalExpressionTestData logicalExpression() {
    return getOrCache(LogicalExpressionTestData.class);
  }

  public WhereAnalyzerTestData whereAnalyzer() {
    return getOrCache(WhereAnalyzerTestData.class);
  }

  public SelectCQLClauseTestData selectCQLClause() {
    return getOrCache(SelectCQLClauseTestData.class);
  }

  public WhereCQLClauseTestData whereCQLClause() {
    return getOrCache(WhereCQLClauseTestData.class);
  }

  public TableUpdateAnalyzerTestData tableUpdateAnalyzer() {
    return getOrCache(TableUpdateAnalyzerTestData.class);
  }
}
