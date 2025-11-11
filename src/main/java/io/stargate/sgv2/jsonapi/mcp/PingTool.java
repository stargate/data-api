package io.stargate.sgv2.jsonapi.mcp;

import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;

public class PingTool {
  @Tool(description = "Simple Ping tool (with configurable message)")
  String ping(
      @ToolArg(description = "Response message", defaultValue = "OK") String responseMessage) {
    return responseMessage;
  }
}
