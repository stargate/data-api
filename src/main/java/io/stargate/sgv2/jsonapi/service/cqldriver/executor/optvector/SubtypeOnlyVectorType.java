package io.stargate.sgv2.jsonapi.service.cqldriver.executor.optvector;

import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import java.util.Objects;

/**
 * An implementation of {@link VectorType} which is only concerned with the subtype of the vector.
 * Useful if you want to describe a call of vector types that do not differ by subtype but do differ
 * by dimension.
 */
public class SubtypeOnlyVectorType extends DefaultVectorType {

  public static final int NO_DIMENSION = -1;

  public SubtypeOnlyVectorType(DataType subtype) {
    super(subtype, NO_DIMENSION);
  }

  @Override
  public int getDimensions() {
    throw new UnsupportedOperationException("Subtype-only vectors do not support dimensions");
  }

  /* ============== General class implementation ============== */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o instanceof VectorType) {
      VectorType that = (VectorType) o;
      return that.getElementType().equals(this.getElementType());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), this.getElementType());
  }

  @Override
  public String toString() {
    return String.format("(Subtype-only) Vector(%s)", getElementType());
  }

  @Override
  public boolean isDetached() {
    return false;
  }

  @Override
  public void attach(AttachmentPoint attachmentPoint) {
    // nothing to do
  }
}
