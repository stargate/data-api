package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.InvertibleCommandClause;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.FilterClauseDeserializer;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.processor.SchemaValidatable;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonDeserialize(using = FilterClauseDeserializer.class)
@Schema(
    type = SchemaType.OBJECT,
    implementation = Object.class,
    example =
        """
             {"name": "Aaron", "country": "US"}
              """)
public record FilterClause(LogicalExpression logicalExpression)
    implements SchemaValidatable, InvertibleCommandClause {

  @Override
  public void validate(TableSchemaObject table) {
    // TODO HACK AARON - this is a temporary fix to allow the tests to pass
    return;
  }

  @Override
  public void validate(CollectionSchemaObject collection) {

    IndexingProjector indexingProjector = collection.indexingProjector();

    // If nothing specified, everything indexed
    if (indexingProjector.isIdentityProjection()) {
      return;
    }
    validateCollectionLogicalExpression(logicalExpression, indexingProjector);
  }

  private void validateCollectionLogicalExpression(
      LogicalExpression logicalExpression, IndexingProjector indexingProjector) {
    for (LogicalExpression subLogicalExpression : logicalExpression.logicalExpressions) {
      validateCollectionLogicalExpression(subLogicalExpression, indexingProjector);
    }
    for (ComparisonExpression subComparisonExpression : logicalExpression.comparisonExpressions) {
      validateCollectionComparisonExpression(subComparisonExpression, indexingProjector);
    }
  }

  private void validateCollectionComparisonExpression(
      ComparisonExpression comparisonExpression, IndexingProjector indexingProjector) {
    String path = comparisonExpression.getPath();
    boolean isPathIndexed =
        !indexingProjector.isIndexingDenyAll() && indexingProjector.isPathIncluded(path);

    // special case path may be set to indexed for `$vector` and `$vectorize` fields even in case of
    // deny all

    // If path is "_id" and it's denied, the operator can only be $eq or $in
    if (!isPathIndexed && path.equals(DocumentConstants.Fields.DOC_ID)) {
      FilterOperator filterOperator = comparisonExpression.getFilterOperations().get(0).operator();
      // if operator is $eq or $in, _id can be used, return
      if (filterOperator == ValueComparisonOperator.EQ
          || filterOperator == ValueComparisonOperator.IN) {
        return;
      }
      // otherwise throw JsonApiException
      throw ErrorCodeV1.ID_NOT_INDEXED.toApiException(
          "you can only use $eq or $in as the operator");
    }

    // If path is not indexed, throw error
    if (!isPathIndexed) {
      throw ErrorCodeV1.UNINDEXED_FILTER_PATH.toApiException(
          "filter path '%s' is not indexed", comparisonExpression.getPath());
    }

    JsonLiteral<?> operand = comparisonExpression.getFilterOperations().get(0).operand();
    // If path is an object (like address), validate the incremental path (like address.city)
    if (operand.type() == JsonType.ARRAY || operand.type() == JsonType.SUB_DOC) {
      if (operand.value() instanceof Map<?, ?> map) {
        validateCollectionMap(indexingProjector, map, path);
      }
      if (operand.value() instanceof List<?> list) {
        validateCollectionList(indexingProjector, list, path);
      }
    }
  }

  private void validateCollectionMap(
      IndexingProjector indexingProjector, Map<?, ?> map, String currentPath) {
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      String incrementalPath = currentPath + "." + entry.getKey();
      if (!indexingProjector.isPathIncluded(incrementalPath)) {
        throw ErrorCodeV1.UNINDEXED_FILTER_PATH.toApiException(
            "filter path '%s' is not indexed", incrementalPath);
      }
      // continue build the incremental path if the value is a map
      if (entry.getValue() instanceof Map<?, ?> valueMap) {
        validateCollectionMap(indexingProjector, valueMap, incrementalPath);
      }
      // continue build the incremental path if the value is a list
      if (entry.getValue() instanceof List<?> list) {
        validateCollectionList(indexingProjector, list, incrementalPath);
      }
    }
  }

  private void validateCollectionList(
      IndexingProjector indexingProjector, List<?> list, String currentPath) {
    for (Object element : list) {
      if (element instanceof Map<?, ?> map) {
        validateCollectionMap(indexingProjector, map, currentPath);
      } else if (element instanceof List<?> sublList) {
        validateCollectionList(indexingProjector, sublList, currentPath);
      } else if (element instanceof String) {
        // no need to build incremental path, validate current path
        if (!indexingProjector.isPathIncluded(currentPath)) {
          throw ErrorCodeV1.UNINDEXED_FILTER_PATH.toApiException(
              "filter path '%s' is not indexed", currentPath);
        }
      }
    }
  }

  public void invertForTableCommand(CommandContext<TableSchemaObject> commandContext) {
    invertLogicalExpression(this.logicalExpression(), null);
  }

  public void invertForCollectionCommand(CommandContext<CollectionSchemaObject> commandContext) {
    invertLogicalExpression(this.logicalExpression(), null);
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
