#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
from http import HTTPStatus
from dashscope import Application

# 设置环境变量
os.environ['DASHSCOPE_API_KEY'] = 'sk-241d5290235f4979b4787a69cd575b9b'
APP_ID = '47d08f89417a4a538bc822b6379e7cf8'

# 准备输入数据
mock_data = [
    {
        "link_id": "test_1",
        "title": "测试新闻标题1",
        "event_tags": ["经济", "科技"]
    },
    {
        "link_id": "test_2", 
        "title": "测试新闻标题2",
        "event_tags": ["科技", "创新"]
    }
]
input_data = {"news_list": mock_data}

# 调用智能体
response = Application.call(
    api_key=os.getenv("DASHSCOPE_API_KEY"),
    app_id=APP_ID,
    prompt=json.dumps(input_data, ensure_ascii=False)
)

# 处理响应
if response.status_code != HTTPStatus.OK:
    print(f'request_id={response.request_id}')
    print(f'code={response.status_code}')
    print(f'message={response.message}')
    print(f'请参考文档：https://help.aliyun.com/zh/model-studio/developer-reference/error-code')
else:
    print(response.output.text) 