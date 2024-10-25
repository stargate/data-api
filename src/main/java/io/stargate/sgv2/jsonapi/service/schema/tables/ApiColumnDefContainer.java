package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDef;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

/** A {@link ApiColumnDefContainer} that maintains the order of the columns as they were added. */
public class ApiColumnDefContainer extends LinkedHashMap<CqlIdentifier, ApiColumnDef> {

  public ApiColumnDefContainer() {
    super();
  }

  public ApiColumnDefContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public ApiColumnDefContainer(ApiColumnDefContainer container) {
    super(container);
  }

  public ApiColumnDefContainer toUnmodifiable() {
    return new UnmodifiableApiColumnDefContainer(this);
  }

  public static ApiColumnDefContainer from(
      Collection<ColumnMetadata> columns, VectorConfig vectorConfig) {
    Objects.requireNonNull(columns, "columns cannot be null");

    var container = new ApiColumnDefContainer(columns.size());
    for (ColumnMetadata columnMetadata : columns) {
      try {
        container.put(
            new ApiColumnDef(
                columnMetadata.getName(), getApiDataType(columnMetadata, vectorConfig)));
      } catch (UnsupportedCqlType e) {
        container.put(
            new ApiColumnDef(
                columnMetadata.getName(), new UnsupportedApiDataType(columnMetadata.getType())));
      }
    }
    return container;
  }

  private static ApiDataType getApiDataType(
      ColumnMetadata columnMetadata, VectorConfig vectorConfig) throws UnsupportedCqlType {

    if (columnMetadata.getType() instanceof VectorType vt) {
      // Special handling because we need to get the vectorize config from the vector config

      // We may not have a vectorize definition for this column, that is OK
      var vectorizeDef =
          vectorConfig
              .getColumnVectorDefinition(columnMetadata.getName().asInternal())
              .map(VectorColumnDefinition::vectorizeDefinition)
              .orElse(null);

      return ComplexApiDataType.ApiVectorType.from(
          ApiDataTypeDefs.from(vt.getElementType()), vt.getDimensions(), vectorizeDef);

    } else {
      return ApiDataTypeDefs.from(columnMetadata.getType());
    }
  }

  public ApiColumnDef put(ApiColumnDef columnDef) {
    return put(columnDef.name(), columnDef);
  }

  public boolean contains(ApiColumnDef columnDef) {
    return containsKey(columnDef.name());
  }

  public ColumnsDef toColumnsDef() {
    ColumnsDef columnsDef = new ColumnsDef(size());
    forEach((name, columnDef) -> columnsDef.put(name, columnDef.type().getColumnType()));
    return columnsDef;
  }

  public static class UnmodifiableApiColumnDefContainer extends ApiColumnDefContainer {

    UnmodifiableApiColumnDefContainer(ApiColumnDefContainer container) {
      super(container);
    }

    @Override
    public ApiColumnDef putFirst(CqlIdentifier cqlIdentifier, ApiColumnDef columnDef) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiColumnDef putLast(CqlIdentifier cqlIdentifier, ApiColumnDef columnDef) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiColumnDef getOrDefault(Object key, ApiColumnDef defaultValue) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<CqlIdentifier, ApiColumnDef> eldest) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(
        BiFunction<? super CqlIdentifier, ? super ApiColumnDef, ? extends ApiColumnDef> function) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiColumnDef put(CqlIdentifier key, ApiColumnDef value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends CqlIdentifier, ? extends ApiColumnDef> m) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiColumnDef remove(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiColumnDef putIfAbsent(CqlIdentifier key, ApiColumnDef value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object key, Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(CqlIdentifier key, ApiColumnDef oldValue, ApiColumnDef newValue) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiColumnDef replace(CqlIdentifier key, ApiColumnDef value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiColumnDef computeIfAbsent(
        CqlIdentifier key,
        Function<? super CqlIdentifier, ? extends ApiColumnDef> mappingFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiColumnDef computeIfPresent(
        CqlIdentifier key,
        BiFunction<? super CqlIdentifier, ? super ApiColumnDef, ? extends ApiColumnDef>
            remappingFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiColumnDef compute(
        CqlIdentifier key,
        BiFunction<? super CqlIdentifier, ? super ApiColumnDef, ? extends ApiColumnDef>
            remappingFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiColumnDef merge(
        CqlIdentifier key,
        ApiColumnDef value,
        BiFunction<? super ApiColumnDef, ? super ApiColumnDef, ? extends ApiColumnDef>
            remappingFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiColumnDef put(ApiColumnDef columnDef) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<CqlIdentifier> keySet() {
      return Collections.unmodifiableSet(super.keySet());
    }

    @Override
    public SequencedSet<CqlIdentifier> sequencedKeySet() {
      return Collections.unmodifiableSequencedSet(super.sequencedKeySet());
    }

    @Override
    public SequencedCollection<ApiColumnDef> sequencedValues() {
      return Collections.unmodifiableSequencedCollection(super.sequencedValues());
    }

    @Override
    public Collection<ApiColumnDef> values() {
      return Collections.unmodifiableCollection(super.values());
    }

    @Override
    public Set<Map.Entry<CqlIdentifier, ApiColumnDef>> entrySet() {
      return Collections.unmodifiableSet(super.entrySet());
    }

    @Override
    public SequencedSet<Map.Entry<CqlIdentifier, ApiColumnDef>> sequencedEntrySet() {
      return Collections.unmodifiableSequencedSet(super.sequencedEntrySet());
    }
  }
}
