/// *
// * Copyright Lealone Database Group.
// * Licensed under the Server Side Public License, v 1.
// * Initial Developer: zhh
// */
// package com.lealone.agent.test;
//
// import java.util.Arrays;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
//
// import com.fasterxml.jackson.core.JsonProcessingException;
// import com.fasterxml.jackson.databind.JsonNode;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.lealone.agent.doubao.DoubaoAgent;
// import com.volcengine.ark.runtime.model.responses.constant.ResponsesConstants;
// import com.volcengine.ark.runtime.model.responses.content.InputContentItemText;
// import com.volcengine.ark.runtime.model.responses.item.BaseItem;
// import com.volcengine.ark.runtime.model.responses.item.ItemEasyMessage;
// import com.volcengine.ark.runtime.model.responses.item.ItemFunctionToolCall;
// import com.volcengine.ark.runtime.model.responses.item.ItemFunctionToolCallOutput;
// import com.volcengine.ark.runtime.model.responses.item.MessageContent;
// import com.volcengine.ark.runtime.model.responses.request.CreateResponsesRequest;
// import com.volcengine.ark.runtime.model.responses.request.ResponsesInput;
// import com.volcengine.ark.runtime.model.responses.response.ResponseObject;
// import com.volcengine.ark.runtime.model.responses.tool.ResponsesTool;
// import com.volcengine.ark.runtime.model.responses.tool.ToolFunction;
// import com.volcengine.ark.runtime.service.ArkService;
//
// public class DoubaoToolTest extends DoubaoAgent {
//
// public static ObjectMapper om = new ObjectMapper();
//
// public static List<ResponsesTool> buildTools() {
// JsonNode params = om.createObjectNode().put("type", "object").set("properties",
// om.createObjectNode().set("location", om.createObjectNode().put("type", "string")
// .put("description", "城市名称，如北京、上海（仅支持国内地级市）")));
//
// ToolFunction t = ToolFunction.builder().name("get_weather")
// .description("根据城市名称查询该城市当日天气（含温度、天气状况）").parameters(params).build();
//
// return Arrays.asList(t);
// }
//
// public static void main(String[] args) throws Exception {
// new DoubaoToolTest().run(args);
// }
//
// public void run(String[] args) throws JsonProcessingException {
// init(args);
// getWeather(args);
// }
//
// public void getWeather(String[] args) throws JsonProcessingException {
// ArkService arkService = getArkService();
// System.out.println("=== First Round Request: Trigger Tool Call ===");
// CreateResponsesRequest req = CreateResponsesRequest.builder().model(model).store(true)
// .input(ResponsesInput.builder().addListItem(ItemEasyMessage.builder()
// .role(ResponsesConstants.MESSAGE_ROLE_USER)
// .content(MessageContent.builder()
// .addListItem(InputContentItemText.builder().text("查询北京今天的天气").build())
// .build())
// .build()).build())
// .tools(buildTools()).build();
// ResponseObject resp = arkService.createResponse(req);
// System.out.println(resp);
// // Extract key information（previous_response_id、call_id、arguments）
// String previousId = resp.getId();
// BaseItem targetCall = null;
// for (BaseItem item : resp.getOutput()) {
// if ("function_call".equals(item.getType())) {
// targetCall = item;
// break;
// }
// }
// ObjectMapper objectMapper = new ObjectMapper();
// String jsonStr = objectMapper.writeValueAsString(targetCall);
// ItemFunctionToolCall functionCall = objectMapper.readValue(jsonStr, ItemFunctionToolCall.class);
// String callId = functionCall.getCallId();
// String callArguments = functionCall.getArguments();
// System.out.println("First Round Response ID:" + previousId);
// System.out.println("Tool Call ID:" + callId);
// System.out.println("Call Parameters:" + callArguments);
//
// System.out.println("=== Simulate Tool Execution: Get Weather Results ===");
// Map<String, String> toolOutput = new HashMap<String, String>() {
// {
// put("city", "北京");
// put("date", "2025-10-13");
// put("temperature", "18~28℃");
// put("condition", "晴转多云");
// put("wind", "东北风2级");
// }
// };
//
// System.out.println(toolOutput);
//
// System.out.println("=== Second Round Request: Return Result and Generate Final Response ===");
// CreateResponsesRequest secondReq = CreateResponsesRequest.builder().model(model)
// .previousResponseId(previousId)
// .input(ResponsesInput.builder()
// .addListItem(ItemFunctionToolCallOutput.builder().callId(callId)
// .output(objectMapper.writeValueAsString(toolOutput)).build())
// .build())
// .tools(buildTools()).build();
// ResponseObject secondResp = arkService.createResponse(secondReq);
// System.out.println("=== Final Answer ===");
// System.out.println(secondResp);
// arkService.shutdownExecutor();
// }
// }
