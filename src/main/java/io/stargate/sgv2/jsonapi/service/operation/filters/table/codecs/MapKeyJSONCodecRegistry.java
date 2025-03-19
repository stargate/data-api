package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.service.resolver.update.TableUpdatePullAllResolver;
import java.util.Collection;

/**
 * Registry designed to be used with the {@link
 * io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator#PULL_ALL} operation where
 * we only have the keys of the map entries to remove, not the map itself. Needs to be handled
 * specially because the CQL type is still a MAP but we want to create a list of values to pass to
 * the CQL operation.
 */
public class MapKeyJSONCodecRegistry extends DefaultJSONCodecRegistry {

  public MapKeyJSONCodecRegistry(DefaultJSONCodecRegistry source) {
    super(source.codecsByCQLType.values().stream().flatMap(Collection::stream).toList());
  }

  /**
   * Overriding to handle the keys where the value we have is a <b>list</b> of keys for a map, not a
   * map itself. This is used when we are removing map entries via the $pullAll.
   *
   * <p>See {@link TableUpdatePullAllResolver} for more details.
   *
   * @param mapType
   * @param value
   * @return
   * @param <JavaT>
   * @param <CqlT>
   * @throws ToCQLCodecException
   */
  @Override
  protected <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(MapType mapType, Object value)
      throws ToCQLCodecException {

    // we have a list of the keys of the map, so we need a codec for a list of the key type
    var listType = new DefaultListType(mapType.getKeyType(), mapType.isFrozen());
    return codecToCQL(listType, value);
  }
}
