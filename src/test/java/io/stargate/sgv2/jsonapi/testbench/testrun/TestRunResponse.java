package io.stargate.sgv2.jsonapi.testbench.testrun;

import io.stargate.sgv2.jsonapi.testbench.messaging.APIRequest;
import io.stargate.sgv2.jsonapi.testbench.messaging.APIResponse;

public record TestRunResponse(
    TestRunRequest testRequest, APIRequest apiRequest, APIResponse apiResponse) {}
