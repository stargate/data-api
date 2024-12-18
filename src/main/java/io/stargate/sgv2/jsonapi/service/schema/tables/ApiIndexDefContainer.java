package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;
import java.util.*;

/** Container for */
public class ApiIndexDefContainer implements Iterable<ApiIndexDef>, PrettyPrintable {

  public static final CqlColumnFactory FROM_CQL_FACTORY = new CqlColumnFactory();

  private static final ApiIndexDefContainer IMMUTABLE_EMPTY =
      new ApiIndexDefContainer(0).toUnmodifiable();

  // Indexes by the target column, we can have multiple indexes for things like maps with indexes on
  // keys, values etc
  private final ListMultimap<CqlIdentifier, ApiIndexDef> byTarget;
  // Indexes by the index name, this is the unique identifier for the index
  private final Map<CqlIdentifier, ApiIndexDef> byName;

  public ApiIndexDefContainer() {
    this.byTarget = ArrayListMultimap.create();
    this.byName = new HashMap<>();
  }

  public ApiIndexDefContainer(int initialCapacity) {
    // most of the time there will only be one index per column
    this.byTarget = ArrayListMultimap.create(initialCapacity, 1);
    this.byName = new HashMap<>(initialCapacity);
  }

  public ApiIndexDefContainer(ApiIndexDefContainer other) {
    Objects.requireNonNull(other, "other must not be null");

    this.byTarget = ArrayListMultimap.create(other.byTarget);
    this.byName = new HashMap<>(other.byName);
  }

  public ApiIndexDefContainer(List<ApiIndexDef> indexDefs) {
    this(indexDefs.size());
    indexDefs.forEach(this::put);
  }

  public static ApiIndexDefContainer of() {
    return IMMUTABLE_EMPTY;
  }

  public ApiIndexDefContainer toUnmodifiable() {
    return new UnmodifiableApiIndexDefContainer(this);
  }

  public void put(ApiIndexDef indexDef) {
    Objects.requireNonNull(indexDef, "indexDef must not be null");

    // if the index is unsupported we do not know the target column and we are not going to be using
    // it anyway
    if (!indexDef.isUnsupported()) {
      byTarget.put(indexDef.targetColumn(), indexDef);
    }
    byName.put(indexDef.indexName(), indexDef);
  }

  public List<CqlIdentifier> allIndexNames() {
    return byName.keySet().stream().toList();
  }

  public List<ApiIndexDef> allIndexes() {
    return byName.values().stream().toList();
  }

  public List<ApiIndexDef> allIndexesFor(CqlIdentifier targetColumn) {
    return Collections.unmodifiableList(byTarget.get(targetColumn));
  }

  public Optional<ApiIndexDef> firstIndexFor(CqlIdentifier targetColumn) {
    var indexes = byTarget.get(targetColumn);
    return Optional.ofNullable(indexes.isEmpty() ? null : indexes.getFirst());
  }

  public ApiIndexDefContainer filterBySupported() {
    return new ApiIndexDefContainer(
            byName.values().stream().filter(i -> !i.isUnsupported()).toList())
        .toUnmodifiable();
  }

  public ApiIndexDefContainer filterByUnsupported() {
    return new ApiIndexDefContainer(
            byName.values().stream().filter(ApiIndexDef::isUnsupported).toList())
        .toUnmodifiable();
  }

  @Override
  public Iterator<ApiIndexDef> iterator() {
    return byName.values().iterator();
  }

  @Override
  public String toString() {
    return toString(false);
  }

  @Override
  public PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
    return prettyToStringBuilder.append("indexes", byName.values());
  }

  public static class CqlColumnFactory {

    CqlColumnFactory() {}

    public ApiIndexDefContainer create(
        ApiColumnDefContainer allColumns, Collection<IndexMetadata> indexes) {
      Objects.requireNonNull(allColumns, "allColumns cannot be null");
      Objects.requireNonNull(indexes, "indexes cannot be null");

      if (indexes.isEmpty()) {
        // re-uses the singleton empty immutable
        return ApiIndexDefContainer.of();
      }

      var container = new ApiIndexDefContainer(indexes.size());
      for (var indexMetadata : indexes) {
        try {
          container.put(IndexFactoryFromCql.DEFAULT.create(allColumns, indexMetadata));
        } catch (UnsupportedCqlIndexException e) {
          container.put(IndexFactoryFromCql.DEFAULT.createUnsupported(indexMetadata));
        }
      }
      return container.toUnmodifiable();
    }
  }

  public static class UnmodifiableApiIndexDefContainer extends ApiIndexDefContainer {

    private UnmodifiableApiIndexDefContainer(ApiIndexDefContainer container) {
      super(container);
    }

    @Override
    public void put(ApiIndexDef indexDef) {
      throw new UnsupportedOperationException();
    }
  }
}
