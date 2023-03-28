package io.stargate.sgv2.jsonapi.service.operation.model;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadDocument;
import io.stargate.sgv2.jsonapi.util.JsonNodeComparator;
import java.util.Comparator;
import java.util.List;

public record ChainedComparator(List<FindOperation.OrderBy> sortColumns)
    implements Comparator<ReadDocument> {
  @Override
  public int compare(ReadDocument o1, ReadDocument o2) {
    int i = 0;
    final List<JsonNode> sortValuesO1 = o1.sortColumns();
    final List<JsonNode> sortValuesO2 = o2.sortColumns();
    for (FindOperation.OrderBy sortColumn : sortColumns()) {
      int compareValue =
          sortColumn.ascending()
              ? JsonNodeComparator.ascending().compare(sortValuesO1.get(i), sortValuesO2.get(i))
              : JsonNodeComparator.descending().compare(sortValuesO1.get(i), sortValuesO2.get(i));
      if (compareValue != 0) return compareValue;
      i++;
    }
    return 0;
  }
}
