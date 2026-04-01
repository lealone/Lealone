/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent.test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

public class OllamaClient {

    public static void main(String[] args) {
        try {
            URL url = URI.create("http://localhost:11434/api/generate").toURL();
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);

            String jsonInputString = "{\"prompt\":\"用java写一个heloworld\"," //
                    + "\"stream\":false," //
                    // + "\"model\":\"deepseek-r1:1.5b\"}";
                    // + "\"model\":\"gemma3:1b\"}";
                    + "\"model\":\"kirito1/qwen3-coder:1.7b\"}";
            connection.getOutputStream().write(jsonInputString.getBytes());

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            System.out.println(response.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
