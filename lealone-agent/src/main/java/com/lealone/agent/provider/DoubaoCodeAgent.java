/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent.provider;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.lealone.agent.AppCodeAgent;
import com.lealone.common.exceptions.DbException;
import com.lealone.orm.json.JsonObject;

//调用LLM的api是低频操作，并且LLM的处理速度很慢，所以直接用HttpURLConnection发送请求处理响应即可
public class DoubaoCodeAgent extends AppCodeAgent {

    public DoubaoCodeAgent() {
        super("doubao");
    }

    @Override
    protected void afterInit() {
        if (model == null)
            model = "doubao-seed-2-0-pro-260215";
        if (url == null)
            // url = "https://ark.cn-beijing.volces.com/api/v3";
            // url = "https://ark.cn-beijing.volces.com/api/v3/chat/completions";
            url = "https://ark.cn-beijing.volces.com/api/v3/responses";
    }

    @Override // Responses API
    public String send(String userPrompt, AtomicReference<String> previousResponseId) {
        try {
            HttpURLConnection connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Authorization", "Bearer " + apiKey);
            connection.setDoOutput(true);
            // connection.setConnectTimeout(10000);
            // connection.setReadTimeout(30000);

            JsonObject reqBody = new JsonObject();
            reqBody.put("model", model);
            reqBody.put("input", userPrompt);
            if (previousResponseId != null && previousResponseId.get() != null
                    && !previousResponseId.get().isBlank()) {
                reqBody.put("previous_response_id", previousResponseId.get());
            }
            reqBody.put("stream", false);
            reqBody.put("thinking", new JsonObject().put("type", "disabled"));

            try (OutputStream os = connection.getOutputStream()) {
                byte[] data = reqBody.toString().getBytes(StandardCharsets.UTF_8);
                os.write(data);
            }

            int code = connection.getResponseCode();
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    code == 200 ? connection.getInputStream() : connection.getErrorStream(),
                    StandardCharsets.UTF_8));
            StringBuilder resp = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                resp.append(line);
            }
            br.close();
            connection.disconnect();
            JsonObject json = new JsonObject(resp.toString());
            if (previousResponseId != null) {
                previousResponseId.set(json.getString("id"));
            }
            List<?> output = (List<?>) json.getMap().get("output");
            Map<?, ?> message = (Map<?, ?>) output.get(0);
            List<?> content = (List<?>) message.get("content");
            Map<?, ?> outputText = (Map<?, ?>) content.get(0);
            return (String) outputText.get("text");
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }
}
