package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Definition of index config */
public class IndexConfig {
  /**
   * Map of index definitions where the key is the column name and the value is the index
   * definition.
   */
  private final Map<String, IndexDefinition> indexDefinitions;

  private IndexConfig(Map<String, IndexDefinition> indexDefinitions) {
    this.indexDefinitions = indexDefinitions;
  }

  public static IndexConfig from(TableMetadata tableMetadata) {
    Map<String, IndexDefinition> indexDefinitions =
        tableMetadata.getIndexes().values().stream()
            .map(
                indexMetadata -> {
                  final ColumnMetadata columnMetadata =
                      tableMetadata
                          .getColumn(
                              CqlIdentifierUtil.cqlIdentifierFromUserInput(
                                  getColumnName(indexMetadata.getTarget())))
                          .orElseThrow(
                              () ->
                                  new IllegalArgumentException(
                                      "Column not found for index: "
                                          + getColumnName(indexMetadata.getTarget())));
                  return IndexDefinition.from(columnMetadata, indexMetadata);
                })
            .collect(
                Collectors.toMap(IndexDefinition::columnName, indexDefinition -> indexDefinition));
    return new IndexConfig(indexDefinitions);
  }

  private static String getColumnName(String target) {
    Pattern pattern = Pattern.compile("\\(([^)]+)\\)");
    Matcher matcher = pattern.matcher(target);
    if (matcher.find()) {
      return matcher.group(1); // Get the content inside the parentheses
    } else {
      return target;
    }
  }

  /** Get index definition by column name */
  public Optional<IndexDefinition> getIndexDefinition(String columnName) {
    return Optional.ofNullable(indexDefinitions.get(columnName));
  }

  public Map<String, IndexDefinition> indexDefinitions() {
    return indexDefinitions;
  }
}
