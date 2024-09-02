package io.stargate.sgv2.jsonapi.mock;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import java.util.*;

/**
 * Helpers to work with lists of {@link ColumnMetadata} and {@link CqlIdentifier}, and to build
 * combinations from lists.
 *
 * <p>I hate calling things util - but could not get a better name - Aaron
 *
 * <p>Some of these functions use generic <code>T</code> just because they can.
 */
public class TestListUtil {

  public static List<CqlIdentifier> columnNames(Collection<ColumnMetadata> columns) {
    if (columns == null) {
      return List.of();
    }
    return columns.stream().map(ColumnMetadata::getName).toList();
  }

  public static <T> List<T> difference(Collection<T> a, Collection<T> b) {
    return a.stream().filter(column -> !b.contains(column)).toList();
  }

  /**
   * Generates test combinations of a list of elements, e.g. for use with test combinations of a
   * list of columns.
   *
   * @param source items we want to generate combinations from
   * @param includeNone if we want to include an empty list
   * @param includeAll if we want to include the full list
   * @return a list of lists of combinations
   * @param <T>
   */
  public static <T> List<List<T>> testCombinations(
      List<T> source, boolean includeNone, boolean includeAll) {
    List<List<T>> combinations = new ArrayList<>();

    if (includeNone) {
      combinations.add(List.of());
    }

    // a combination that only has one element
    // if there is only one element, do not include unless we want to include all
    if (source.size() > 1 || includeAll) {
      source.forEach(element -> combinations.add(List.of(element)));
    }

    if (source.size() > 1) {
      // a combination that has all elements except one
      source.forEach(
          element -> {
            List<T> combination = new ArrayList<>(source);
            combination.remove(element);
            combinations.add(combination);
          });

      // if size is 1 the loop for one element combinations will cover this
      if (includeAll) {
        combinations.add(source);
      }
    }
    return combinations;
  }

  @SafeVarargs
  public static <T> List<T> join(Collection<T>... lists) {
    return Arrays.stream(lists).flatMap(Collection::stream).toList();
  }
}
