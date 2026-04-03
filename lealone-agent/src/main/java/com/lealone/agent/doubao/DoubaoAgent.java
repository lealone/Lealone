/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent.doubao;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import java.util.Map;

import com.lealone.agent.CodeAgentBase;
import com.lealone.common.exceptions.DbException;
import com.lealone.orm.json.JsonArray;
import com.lealone.orm.json.JsonObject;

//调用LLM的api是低频操作，并且LLM的处理速度很慢，所以直接用HttpURLConnection发送请求处理响应即可
public class DoubaoAgent extends CodeAgentBase {

    public DoubaoAgent() {
        super("doubao");
    }

    @Override
    public void init(String[] args) {
        super.init(args);
        afterInit();
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        afterInit();
    }

    private void afterInit() {
        if (model == null)
            model = "doubao-seed-2-0-pro-260215";
        if (url == null)
            // url = "https://ark.cn-beijing.volces.com/api/v3";
            url = "https://ark.cn-beijing.volces.com/api/v3/chat/completions";
    }

    // public ArkService getArkService() {
    // return ArkService.builder().apiKey(apiKey).baseUrl(url).build();
    // }
    //
    // public String generateJavaCode2(String userPrompt) {
    // ArkService arkService = getArkService();
    //
    // userPrompt = """
    // 请根据需求生成Java代码实现类不要生成接口，只输出纯代码，不要任何解释，不要```java标记，不要多余文字：
    // """ + userPrompt;
    //
    // CreateResponsesRequest request = CreateResponsesRequest.builder().model(model)
    // .input(ResponsesInput.builder().stringValue(userPrompt).build())
    // // Manually disable deep thinking
    // .thinking(ResponsesThinking.builder().type(ResponsesConstants.THINKING_TYPE_DISABLED)
    // .build())
    // .build();
    //
    // ResponseObject resp = arkService.createResponse(request);
    // String javaCode = "";
    // for (BaseItem item : resp.getOutput()) {
    // if (ResponsesConstants.ITEM_TYPE_MESSAGE.equals(item.getType())) {
    // ItemOutputMessage message = (ItemOutputMessage) item;
    // javaCode = ((OutputContentItemText) message.getContent().get(0)).getText();
    // break;
    // }
    // }
    //
    // arkService.shutdownExecutor();
    // return javaCode;
    // }

    @Override
    public String generateJavaCode(String userPrompt) {
        try {
            userPrompt = """
                    请根据需求生成Java代码实现类不要生成接口，只输出纯代码，不要任何解释，不要```java标记，不要多余文字：

                    """ + userPrompt;
            HttpURLConnection connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Authorization", "Bearer " + apiKey);
            connection.setDoOutput(true);
            JsonArray messages = new JsonArray();
            JsonObject message = new JsonObject();
            message.put("role", "user");
            message.put("content", userPrompt);
            messages.add(message);
            JsonObject json = new JsonObject();
            json.put("model", model);
            json.put("messages", messages);
            json.put("thinking", new JsonObject().put("type", "disabled"));
            try (OutputStream out = connection.getOutputStream()) {
                out.write(json.encode().getBytes("UTF-8"));
            }
            try (InputStream is = connection.getInputStream()) {
                BufferedReader in = new BufferedReader(new InputStreamReader(is));
                String inputLine;
                StringBuilder response = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                json = new JsonObject(response.toString());
                List<?> choices = (List<?>) json.getMap().get("choices");
                Map<?, ?> choice = (Map<?, ?>) choices.get(0);
                Map<?, ?> responseMessage = (Map<?, ?>) choice.get("message");
                String content = (String) responseMessage.get("content");
                return content;
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }
}
