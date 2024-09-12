package io.stargate.sgv2.jsonapi.service.shredding;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import java.util.Collection;

/**
 * A {@link CqlNamedValueContainer} container that maintains the order the named values were added.
 */
public class OrderedCqlNamedValueContainer
    extends OrderedNamedValueContainer<ColumnMetadata, Object, CqlNamedValue>
    implements CqlNamedValueContainer {

  public OrderedCqlNamedValueContainer() {
    super();
  }

  public OrderedCqlNamedValueContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public OrderedCqlNamedValueContainer(CqlNamedValueContainer container) {
    super(container);
  }

  public OrderedCqlNamedValueContainer(Collection<CqlNamedValue> values) {
    super();
    putAll(values);
  }
}
