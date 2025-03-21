package io.stargate.sgv2.jsonapi.util;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.CqlVector;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility functions for getting strings and values to print from {@link
 * com.datastax.oss.driver.api.core.cql.SimpleStatement}
 *
 * <p>Trims the long values we get with vectors etc.
 */
public abstract class CqlPrintUtil {

  public static final int MAX_VECTOR_SIZE = 5;

  public static List<Object> trimmedPositionalValues(SimpleStatement statement) {
    return statement.getPositionalValues().stream()
        .map(
            value -> {
              if (value instanceof CqlVector<?> vector) {
                int vectorSize = vector.size();
                List<Object> trimmedList = new ArrayList<>();

                // Add elements up to a maximum of MAX_VECTOR_SIZE or the actual vector size,
                // whichever is
                // smaller
                for (int i = 0; i < Math.min(MAX_VECTOR_SIZE, vectorSize); i++) {
                  trimmedList.add(vector.get(i));
                }
                if (trimmedList.size() < vectorSize) {
                  trimmedList.add(
                      "<vector<%s> trimmed>".formatted(vector.size()));
                }
                return trimmedList;
              }
              return value;
            })
        .toList();
  }

  public static String trimmedCql(SimpleStatement statement) {
    // ANN OF [-0.042139724, 0.020535178, 0.06071997, 0.06071997, 0.06071997 ... ]
    var cql = statement.getQuery();
    int start = cql.indexOf("ANN OF [");
    if (start > -1) {
      int end = cql.indexOf("]", start);

      var floatPos = start;
      for (int i = 0; i < MAX_VECTOR_SIZE; i++) {
        var nextPos = cql.indexOf(",", floatPos + 1, end);
        if (nextPos > -1) {
          floatPos = nextPos;
        } else {
          floatPos = end; // before ']', this is to include the last float we want to log
          break;
        }
      }
      cql = cql.substring(0, floatPos) + ", <vector<unknown> trimmed>" + cql.substring(end);
    }
    return cql;
  }
}
