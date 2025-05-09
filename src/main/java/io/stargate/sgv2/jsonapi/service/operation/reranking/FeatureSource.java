package io.stargate.sgv2.jsonapi.service.operation.reranking;

public interface FeatureSource {
  FeatureUsage getFeatureUsage();
  //  void trackFeatureUsgae(FeautureUsage featureUsage) { return featureUsage; }
}
