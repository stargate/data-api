package io.stargate.sgv2.jsonapi.service.shredding;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlVector;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableOrderByANNCqlClause;
import io.stargate.sgv2.jsonapi.util.CqlVectorUtil;

/**
 * Subclass of {@link CqlVectorNamedValue} for use with the read code pathway when deferring values
 * for vectorizing.
 *
 * <p>When building the ANN order by with {@link TableOrderByANNCqlClause} the driver wants a {@link
 * CqlVector} rather than the <code>float[]</code> the codecs create for inserting. So this class is
 * to do the conversion from <code>float[]</code> to {@link CqlVector} in a better place and closer
 * to the value than in the clause itself.
 *
 * <p>Call {@link #cqlVector()} to get the {@link CqlVector} after the value has been prepared, if
 * the value is not prepared this will throw an exception same as {@link CqlNamedValue#value()}.
 */
public class CqlVectorNamedValue extends CqlNamedValue {

  private CqlVector<Float> cqlVector = null;

  public CqlVectorNamedValue(
      CqlIdentifier name,
      JSONCodecRegistry codecRegistry,
      ErrorStrategy<? extends RequestException> errorStrategy) {
    super(name, codecRegistry, errorStrategy);
  }

  @Override
  protected void setDecodedValue(Object value) {
    // pass through so the untyped value is set and the status is updated
    super.setDecodedValue(value);

    // we know this should be a float[] from the codec
    cqlVector = CqlVectorUtil.floatsToCqlVector((float[]) value);
  }

  public CqlVector<Float> cqlVector() {
    checkIsState(NamedValueState.PREPARED, "getCqlVector()");
    return cqlVector;
  }
}
