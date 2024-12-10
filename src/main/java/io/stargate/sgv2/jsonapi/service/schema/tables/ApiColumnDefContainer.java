package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlColumn;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserColumn;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link ApiColumnDefContainer} that maintains the order of the columns as they were added. */
public class ApiColumnDefContainer extends LinkedHashMap<CqlIdentifier, ApiColumnDef> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiColumnDefContainer.class);

  private static final ApiColumnDefContainer IMMUTABLE_EMPTY =
      new ApiColumnDefContainer(0).toUnmodifiable();

  public static final CqlColumnFactory FROM_CQL_FACTORY = new CqlColumnFactory();
  public static final ColumnDescFactory FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();

  public ApiColumnDefContainer() {
    super();
  }

  public ApiColumnDefContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public ApiColumnDefContainer(ApiColumnDefContainer container) {
    super(container);
  }

  public ApiColumnDefContainer(Map<CqlIdentifier, ApiColumnDef> entries) {
    super(entries);
  }

  public ApiColumnDefContainer(List<ApiColumnDef> columnDefs) {
    super(columnDefs.size());
    columnDefs.forEach(this::put);
  }

  public static ApiColumnDefContainer of(ApiColumnDefContainer container) {
    return new ApiColumnDefContainer(container).toUnmodifiable();
  }

  public static ApiColumnDefContainer of(List<ApiColumnDef> columnDefs) {
    return new ApiColumnDefContainer(columnDefs).toUnmodifiable();
  }

  public static ApiColumnDefContainer of() {
    return IMMUTABLE_EMPTY;
  }

  public static ApiColumnDefContainer of(ApiColumnDef col1) {
    return new ApiColumnDefContainer(List.of(col1)).toUnmodifiable();
  }

  public static ApiColumnDefContainer of(ApiColumnDef col1, ApiColumnDef col2) {
    return new ApiColumnDefContainer(List.of(col1, col2)).toUnmodifiable();
  }

  public static ApiColumnDefContainer of(ApiColumnDef col1, ApiColumnDef col2, ApiColumnDef col3) {
    return new ApiColumnDefContainer(List.of(col1, col2, col3)).toUnmodifiable();
  }

  public static ApiColumnDefContainer of(
      ApiColumnDef col1, ApiColumnDef col2, ApiColumnDef col3, ApiColumnDef col4) {
    return new ApiColumnDefContainer(List.of(col1, col2, col3, col4)).toUnmodifiable();
  }

  public ApiColumnDefContainer toUnmodifiable() {
    return this instanceof UnmodifiableApiColumnDefContainer
        ? this
        : new UnmodifiableApiColumnDefContainer(this);
  }

  public ApiColumnDef put(ApiColumnDef columnDef) {
    Objects.requireNonNull(columnDef, "columnDef cannot be null");
    return put(columnDef.name(), columnDef);
  }

  public boolean contains(ApiColumnDef columnDef) {
    Objects.requireNonNull(columnDef, "columnDef cannot be null");
    return containsKey(columnDef.name());
  }

  public List<CqlIdentifier> identifiers() {
    return keySet().stream().toList();
  }

  public List<ApiColumnDef> filterBySupportedTypeToList(ApiTypeName type) {
    return values().stream()
        .filter(
            columnDef -> !columnDef.type().isUnsupported() && columnDef.type().typeName() == type)
        .toList();
  }

  public ApiColumnDefContainer filterBySupported(ApiTypeName type) {
    return new ApiColumnDefContainer(filterBySupportedTypeToList(type));
  }

  public ApiColumnDefContainer filterBySupported(Collection<CqlIdentifier> identifiers) {
    return new ApiColumnDefContainer(
        values().stream().filter(columnDef -> identifiers.contains(columnDef.name())).toList());
  }

  public ApiColumnDefContainer filterByUnsupported() {
    return new ApiColumnDefContainer(
        entrySet().stream()
            .filter(entry -> entry.getValue().type().isUnsupported())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  public Map<CqlIdentifier, VectorizeDefinition> getVectorizeDefs() {
    return filterBySupportedTypeToList(ApiTypeName.VECTOR).stream()
        .filter(
            columnDef ->
                columnDef.type() instanceof ApiVectorType vt && vt.getVectorizeDefinition() != null)
        .collect(
            Collectors.toMap(
                ApiColumnDef::name,
                columnDef -> ((ApiVectorType) columnDef.type()).getVectorizeDefinition()));
  }

  public ColumnsDescContainer toColumnsDesc() {
    ColumnsDescContainer columnsDesc = new ColumnsDescContainer(size());
    forEach((name, columnDef) -> columnsDesc.put(name, columnDef.type().columnDesc()));
    return columnsDesc;
  }

  public static class CqlColumnFactory {

    CqlColumnFactory() {}

    public ApiColumnDefContainer create(
        Collection<ColumnMetadata> columns, VectorConfig vectorConfig) {
      Objects.requireNonNull(columns, "columns cannot be null");

      var container = new ApiColumnDefContainer(columns.size());
      for (ColumnMetadata columnMetadata : columns) {
        try {
          container.put(ApiColumnDef.FROM_CQL_FACTORY.create(columnMetadata, vectorConfig));
        } catch (UnsupportedCqlColumn e) {
          container.put(ApiColumnDef.FROM_CQL_FACTORY.createUnsupported(columnMetadata));
        }
      }
      return container.toUnmodifiable();
    }
  }

  public static class ColumnDescFactory {

    ColumnDescFactory() {}

    public ApiColumnDefContainer create(
        ColumnsDescContainer columnDescContainer, VectorizeConfigValidator validateVectorize) {
      Objects.requireNonNull(columnDescContainer, "columnDescContainer cannot be null");

      var container = new ApiColumnDefContainer(columnDescContainer.size());
      for (Map.Entry<String, ColumnDesc> entry : columnDescContainer.entrySet()) {
        try {
          container.put(
              ApiColumnDef.FROM_COLUMN_DESC_FACTORY.create(
                  entry.getKey(), entry.getValue(), validateVectorize));
        } catch (UnsupportedUserColumn e) {
          container.put(
              ApiColumnDef.FROM_COLUMN_DESC_FACTORY.createUnsupported(
                  entry.getKey(), entry.getValue()));
        }
      }
      return container.toUnmodifiable();
    }
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
