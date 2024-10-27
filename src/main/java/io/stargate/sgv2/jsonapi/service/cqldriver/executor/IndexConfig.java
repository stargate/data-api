package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.service.schema.tables.IndexDefinition;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Map of index definitions where the key is the column name and the value is the index definition.
 */
public class IndexConfig extends HashMap<CqlIdentifier, IndexDefinition> {

  private IndexConfig(Map<CqlIdentifier, IndexDefinition> indexDefinitions) {
    super(indexDefinitions);
  }

  public static IndexConfig from(TableMetadata tableMetadata) {
    return new IndexConfig(
        tableMetadata.getIndexes().values().stream()
            .map(
                indexMetadata ->
                    IndexDefinition.from(
                        tableMetadata
                            .getColumn(
                                CqlIdentifierUtil.cqlIdentifierFromUserInput(
                                    getColumnName(indexMetadata.getTarget())))
                            .orElseThrow(
                                () ->
                                    new IllegalArgumentException(
                                        "Column not found for index: "
                                            + getColumnName(indexMetadata.getTarget()))),
                        indexMetadata))
            .collect(
                Collectors.toMap(
                    IndexDefinition::getColumnName, indexDefinition -> indexDefinition)));
  }

  // Regex pattern to extract the column name from the index target
  private static Pattern pattern = Pattern.compile("\\(([^)]+)\\)");

  /**
   * This is to extract the column name from the index target, which may contain column names for
   * primitive indexes and indexType(column name) for collection types. Eg: values(column),
   * keys(column), full(column) and entries(column)
   *
   * @param target
   * @return
   */
  private static String getColumnName(String target) {
    Matcher matcher = pattern.matcher(target);
    if (matcher.find()) {
      return matcher.group(1); // Get the content inside the parentheses
    } else {
      return target;
    }
  }
}
