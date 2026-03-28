/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent.doubao;

import java.util.Map;

import com.lealone.agent.CodeAgentBase;
import com.volcengine.ark.runtime.model.responses.common.ResponsesThinking;
import com.volcengine.ark.runtime.model.responses.constant.ResponsesConstants;
import com.volcengine.ark.runtime.model.responses.content.OutputContentItemText;
import com.volcengine.ark.runtime.model.responses.item.BaseItem;
import com.volcengine.ark.runtime.model.responses.item.ItemOutputMessage;
import com.volcengine.ark.runtime.model.responses.request.CreateResponsesRequest;
import com.volcengine.ark.runtime.model.responses.request.ResponsesInput;
import com.volcengine.ark.runtime.model.responses.response.ResponseObject;
import com.volcengine.ark.runtime.service.ArkService;

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
            url = "https://ark.cn-beijing.volces.com/api/v3";
    }

    public ArkService getArkService() {
        return ArkService.builder().apiKey(apiKey).baseUrl(url).build();
    }

    @Override
    public String generateJavaCode(String userPrompt) {
        ArkService arkService = getArkService();

        userPrompt = """
                请根据需求生成Java代码实现类不要生成接口，只输出纯代码，不要任何解释，不要```java标记，不要多余文字：
                """ + userPrompt;

        CreateResponsesRequest request = CreateResponsesRequest.builder().model(model)
                .input(ResponsesInput.builder().stringValue(userPrompt).build())
                // Manually disable deep thinking
                .thinking(ResponsesThinking.builder().type(ResponsesConstants.THINKING_TYPE_DISABLED)
                        .build())
                .build();

        ResponseObject resp = arkService.createResponse(request);
        String javaCode = "";
        for (BaseItem item : resp.getOutput()) {
            if (ResponsesConstants.ITEM_TYPE_MESSAGE.equals(item.getType())) {
                ItemOutputMessage message = (ItemOutputMessage) item;
                javaCode = ((OutputContentItemText) message.getContent().get(0)).getText();
                break;
            }
        }

        arkService.shutdownExecutor();
        return javaCode;
    }
}
