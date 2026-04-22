package io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun;

import java.util.function.BooleanSupplier;

public interface TestExecutionCondition extends BooleanSupplier {

    void abortFutureTests(String message);

    String message();

    public class Default implements TestExecutionCondition {

        private boolean condition = true;
        private String message = "";

        // to scope of this condition, i.e. this is for
        // TestEnv: [MODEL=NV-Embed-QA, PROVIDER=nvidia]
        private String scope;

        public Default(String scope) {
            this.scope = scope;
        }
        @Override
        public void abortFutureTests(String message) {
            condition = false;
            this.message = message;
        }

        @Override
        public boolean getAsBoolean() {
            return condition;
        }

        @Override
        public String message() {
            return "TestCondition: Scope=" + scope + ", Message=" + message;
        }
    }

    public class AlwaysTrue implements TestExecutionCondition {

        // to scope of this condition, i.e. this is for
        // TestEnv: [MODEL=NV-Embed-QA, PROVIDER=nvidia]
        // we never need a message, because always true, but here for debugging.
        private String scope;

        public AlwaysTrue(String scope) {
            this.scope = scope;
        }

        @Override
        public void abortFutureTests(String message) {}

        @Override
        public boolean getAsBoolean() {
            return true;
        }

        @Override
        public String message() {
            return "";
        }
    }
}