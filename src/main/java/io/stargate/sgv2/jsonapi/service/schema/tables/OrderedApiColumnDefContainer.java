package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.LinkedHashMap;

/** A {@link ApiColumnDefContainer} that maintains the order of the columns as they were added. */
public class OrderedApiColumnDefContainer extends LinkedHashMap<CqlIdentifier, ApiColumnDef>
    implements ApiColumnDefContainer {

  public OrderedApiColumnDefContainer() {
    super();
  }

  public OrderedApiColumnDefContainer(int initialCapacity) {
    super(initialCapacity);
  }
}
