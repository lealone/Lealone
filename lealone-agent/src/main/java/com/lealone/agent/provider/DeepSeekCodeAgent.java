/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent.provider;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.lealone.agent.AppCodeAgent;
import com.lealone.common.exceptions.DbException;
import com.lealone.orm.json.JsonArray;
import com.lealone.orm.json.JsonObject;

public class DeepSeekCodeAgent extends AppCodeAgent {

    public DeepSeekCodeAgent() {
        super("deepseek");
    }

    @Override
    protected void afterInit() {
        if (model == null)
            model = "deepseek-v4-flash";
        if (url == null)
            url = "https://api.deepseek.com/chat/completions";
    }

    @Override
    public String send(String userPrompt, AtomicReference<String> previousResponseId) {
        try {
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
            json.put("stream", false);
            try (OutputStream out = connection.getOutputStream()) {
                out.write(json.encode().getBytes("UTF-8"));
            }
            try (InputStream is = connection.getInputStream()) {
                BufferedReader in = new BufferedReader(new InputStreamReader(is, "UTF-8"));
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
