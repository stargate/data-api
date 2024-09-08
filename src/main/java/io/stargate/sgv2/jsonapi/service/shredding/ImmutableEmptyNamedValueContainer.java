package io.stargate.sgv2.jsonapi.service.shredding;

import java.util.*;

public class ImmutableEmptyNamedValueContainer <NameT, ValueT, NvT extends NamedValue<NameT, ValueT>>
    implements NamedValueContainer<NameT, ValueT, NvT>{

  static UnsupportedOperationException uoe() { return new UnsupportedOperationException(); }


  public NamedValueContainer<NameT, ValueT, NvT> newContainer() {
    throw uoe();
  }

  @Override
  public NamedValue<NameT, ValueT> put(NvT namedValue) {
    throw uoe();
  }

  // override all methods of the Map interface and throw UnsupportedOperationException
  @Override
  public NvT put(NameT key, NvT value) {
    throw uoe();
  }

  @Override
  public void putAll(Map<? extends NameT, ? extends NvT> m) {
    throw uoe();
  }

  @Override
  public NvT remove(Object key) {
    throw uoe();
  }

  @Override
  public void clear() {
    throw uoe();
  }

  // Override other modifying methods and make the rest return immutable results
  @Override
  public Set<Entry<NameT, NvT>> entrySet() {
    return Collections.emptySet();
  }

  @Override
  public Set<NameT> keySet() {
    return Collections.emptySet();
  }

  @Override
  public Collection<NvT> values() {
    return Collections.emptyList();
  }

  @Override
  public NvT get(Object key) {
    return null;
  }

  @Override
  public boolean containsKey(Object key) {
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    return false;
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

  @Override
  public int size() {
    return 0;
  }
}
