package io.stargate.sgv2.jsonapi.api.v1.vectorize.messaging;

import io.restassured.response.ValidatableResponse;

public record APIResponse(APIRequest apiRequest, ValidatableResponse validatable) {}
