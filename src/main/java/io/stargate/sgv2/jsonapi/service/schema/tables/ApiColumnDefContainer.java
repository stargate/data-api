package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescribable;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlColumn;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserColumn;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** A {@link ApiColumnDefContainer} that maintains the order of the columns as they were added. */
public class ApiColumnDefContainer extends LinkedHashMap<CqlIdentifier, ApiColumnDef>
    implements SchemaDescribable<ColumnsDescContainer>, Recordable {

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

  /** Filter the columns by support matcher to a list */
  public List<ApiColumnDef> filterBySupportToList(Predicate<ApiSupportDef> matcher) {
    return values().stream()
        .filter(columnDef -> matcher.test(columnDef.type().apiSupport()))
        .toList();
  }

  /** Filter the columns by support matcher to an ApiColumnDefContainer */
  public ApiColumnDefContainer filterBySupportToContainer(Predicate<ApiSupportDef> matcher) {
    return new ApiColumnDefContainer(filterBySupportToList(matcher));
  }

  /** Filter the columns by identifiers without checking the support */
  public ApiColumnDefContainer filterByIdentifiers(Collection<CqlIdentifier> identifiers) {
    return new ApiColumnDefContainer(
        values().stream().filter(columnDef -> identifiers.contains(columnDef.name())).toList());
  }

  /** Filter the vector columns by support matcher to a list */
  public List<ApiColumnDef> filterVectorColumnsToList() {
    return filterBySupportToContainer(
            ApiSupportDef.Matcher.NO_MATCHES.withCreateTable(true).withInsert(true).withRead(true))
        .values()
        .stream()
        .filter(columnDef -> columnDef.type().typeName() == ApiTypeName.VECTOR)
        .toList();
  }

  /** Filter the vector columns by support matcher to an ApiColumnDefContainer */
  public ApiColumnDefContainer filterVectorColumnsToContainer() {
    return new ApiColumnDefContainer(filterVectorColumnsToList());
  }

  public Map<CqlIdentifier, VectorizeDefinition> getVectorizeDefs() {
    return filterVectorColumnsToList().stream()
        .filter(
            columnDef ->
                columnDef.type() instanceof ApiVectorType vt && vt.getVectorizeDefinition() != null)
        .collect(
            Collectors.toMap(
                ApiColumnDef::name,
                columnDef -> ((ApiVectorType) columnDef.type()).getVectorizeDefinition()));
  }

  @Override
  public ColumnsDescContainer getSchemaDescription(SchemaDescSource schemaDescSource) {

    ColumnsDescContainer columnsDesc = new ColumnsDescContainer(size());
    forEach(
        (name, columnDef) ->
            columnsDesc.put(name, columnDef.getSchemaDescription(schemaDescSource)));
    return columnsDesc;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    // only want to include the ApiColumnDefs
    return dataRecorder.append("columns", this.values());
  }

  /**
   * Factory for creating {@link ApiColumnDefContainer} from the driver / cql metadata.
   *
   * <p>...
   */
  public static class CqlColumnFactory {

    CqlColumnFactory() {}

    public ApiColumnDefContainer create(
        TypeBindingPoint bindingPoint,
        Collection<ColumnMetadata> columns,
        VectorConfig vectorConfig) {

      Objects.requireNonNull(columns, "columns cannot be null");

      if (bindingPoint != TypeBindingPoint.TABLE_COLUMN
          && bindingPoint != TypeBindingPoint.UDT_FIELD) {
        throw new IllegalArgumentException(
            "CqlColumnFactory only supports binding point %s or %s, bindingPoint: %s"
                .formatted(
                    TypeBindingPoint.TABLE_COLUMN, TypeBindingPoint.UDT_FIELD, bindingPoint));
      }

      var container = new ApiColumnDefContainer(columns.size());
      for (ColumnMetadata columnMetadata : columns) {
        try {
          container.put(
              ApiColumnDef.FROM_CQL_FACTORY.create(bindingPoint, columnMetadata, vectorConfig));
        } catch (UnsupportedCqlColumn e) {
          container.put(ApiColumnDef.FROM_CQL_FACTORY.createUnsupported(columnMetadata));
        }
      }
      return container.toUnmodifiable();
    }
  }

  /**
   * Factory for creating {@link ApiColumnDefContainer} from the user provided descriptions in the
   * API.
   *
   * <p>..
   */
  public static class ColumnDescFactory {

    ColumnDescFactory() {}

    public ApiColumnDefContainer create(
        TypeBindingPoint bindingPoint,
        ColumnsDescContainer columnDescContainer,
        VectorizeConfigValidator validateVectorize) {

      Objects.requireNonNull(columnDescContainer, "columnDescContainer cannot be null");
      checkBindingPoint(bindingPoint, "create");

      var container = new ApiColumnDefContainer(columnDescContainer.size());
      for (Map.Entry<String, ColumnDesc> entry : columnDescContainer.entrySet()) {
        try {
          container.put(
              ApiColumnDef.FROM_COLUMN_DESC_FACTORY.create(
                  bindingPoint, entry.getKey(), entry.getValue(), validateVectorize));
        } catch (UnsupportedUserColumn e) {
          container.put(
              ApiColumnDef.FROM_COLUMN_DESC_FACTORY.createUnsupported(
                  entry.getKey(), entry.getValue()));
        }
      }
      return container.toUnmodifiable();
    }

    public List<ColumnDesc> unbindableColumnDesc(
        TypeBindingPoint bindingPoint,
        ColumnsDescContainer columnDescContainer,
        VectorizeConfigValidator validateVectorize) {

      List<ColumnDesc> unbindable = new ArrayList<>();

      for (Map.Entry<String, ColumnDesc> entry : columnDescContainer.entrySet()) {
        if (!ApiColumnDef.FROM_COLUMN_DESC_FACTORY.isTypeBindable(
            bindingPoint, entry.getKey(), entry.getValue(), validateVectorize)) {
          unbindable.add(entry.getValue());
        }
      }
      return unbindable;
    }

    private static void checkBindingPoint(TypeBindingPoint bindingPoint, String methodName) {
      if (bindingPoint != TypeBindingPoint.TABLE_COLUMN
          && bindingPoint != TypeBindingPoint.UDT_FIELD) {
        throw bindingPoint.unsupportedException("ApiColumnDef.ColumnDescFactory." + methodName);
      }
    }
  }

  /**
   * An unmodifiable version of {@link ApiColumnDefContainer}
   *
   * <p>...
   */
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
