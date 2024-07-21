package io.stargate.sgv2.jsonapi.exception.playing;

// TODO has all the fields we want to return in the error object in the request
// TODO: where do we implement error coalescing , here or in in the APIException
public record CommandResponseError(
    String family, String scope, String code, String title, String message) {}
