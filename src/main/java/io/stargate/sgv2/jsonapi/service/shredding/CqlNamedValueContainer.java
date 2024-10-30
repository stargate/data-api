package io.stargate.sgv2.jsonapi.service.shredding;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import java.util.Collection;
import java.util.List;

/** A container for {@link CqlNamedValue}'s that maintains the order the named values were added. */
public class CqlNamedValueContainer
    extends NamedValueContainer<ColumnMetadata, Object, CqlNamedValue> {

  public CqlNamedValueContainer() {
    super();
  }

  public CqlNamedValueContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public CqlNamedValueContainer(CqlNamedValueContainer container) {
    super(container);
  }

  public CqlNamedValueContainer(Collection<CqlNamedValue> values) {
    super();
    putAll(values);
  }

  public List<CqlIdentifier> getIdentifiers() {
    return keySet().stream().map(ColumnMetadata::getName).toList();
  }
}
