package io.stargate.sgv2.jsonapi.grpc.retries.impl;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.grpc.retries.GrpcRetryPredicate;
import java.util.Objects;
import javax.enterprise.context.ApplicationScoped;

/** Default gRPC retry policy used in the project. */
@ApplicationScoped
public class JsonApiGrpcRetryPolicy implements GrpcRetryPredicate {

  private static final Metadata.Key<QueryOuterClass.WriteTimeout> WRITE_TIMEOUT_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.WriteTimeout.getDefaultInstance());

  private static final Metadata.Key<QueryOuterClass.ReadTimeout> READ_TIMEOUT_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.ReadTimeout.getDefaultInstance());

  /** {@inheritDoc} */
  @Override
  public boolean test(StatusRuntimeException e) {
    Status status = e.getStatus();
    Status.Code code = status.getCode();

    // always retry unavailable
    if (Objects.equals(code, Status.Code.UNAVAILABLE)) {
      return true;
    }

    // for timeouts, retry only server side timeouts
    if (Objects.equals(code, Status.Code.DEADLINE_EXCEEDED)) {
      return isValidServerSideTimeout(e.getTrailers());
    }

    // nothing else
    return false;
  }

  // ensure we retry only server side timeouts we want
  private boolean isValidServerSideTimeout(Metadata trailers) {
    // if we have trailers
    if (null != trailers) {
      // TODO double check the CAS write timeout retries are fine
      return trailers.containsKey(READ_TIMEOUT_KEY) || trailers.containsKey(WRITE_TIMEOUT_KEY);
    }

    // otherwise not
    return false;
  }
}
