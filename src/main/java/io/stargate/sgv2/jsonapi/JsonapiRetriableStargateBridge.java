package io.stargate.sgv2.jsonapi;

import io.quarkus.grpc.GrpcService;
import io.stargate.sgv2.api.common.config.GrpcConfig;
import io.stargate.sgv2.api.common.grpc.qualifier.Retriable;
import io.stargate.sgv2.api.common.grpc.retries.GrpcRetryPredicate;
import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Produces;

@Alternative
@Priority(1)
@GrpcService
@Singleton
public class JsonapiRetriableStargateBridge {

  @Produces
  @Retriable
  io.stargate.sgv2.api.common.grpc.RetriableStargateBridge retriableStargateBridge(
      Instance<GrpcRetryPredicate> predicate, GrpcConfig grpcConfig) {
    return new io.stargate.sgv2.api.common.grpc.RetriableStargateBridge(
        null, predicate.get(), grpcConfig);
  }
}
