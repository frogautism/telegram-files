# Filter Expression 使用指南

- Filter Expression 使用 Python 后端的安全表达式求值器，允许用户编写灵活的过滤规则来筛选消息。
- 表达式必须返回布尔值，表达式为 true 的消息会被保留（通过）。
- 可以直接访问 TdApi.Message 的字段（如 id、chatId、date、content 等）以及注入的 `f`（FileRecord）。
- 可使用已注册的命名空间函数（如 str:contains(...)、re:isMatch(...)、num:... 等）。

常用操作符与调用形式

- 逻辑：and, or, not
- 比较：==, !=, >, <, >=, <=
- 算术：+ - * / %
- 命名空间函数：namespace:method(arg1, arg2)（示例：str:contains(...)、re:isMatch(...)）
- 对象方法调用：f.size(), f.fileName(), f.downloadStatus(), content.text.text 等

命名空间

- str（字符串工具，例如 contains、startWith、isBlank）
- re（正则工具，例如 isMatch）
- num、date、coll、array、obj 等（可用来处理数值、日期、集合、数组、对象）

表达式示例

文本相关（Message.content.text）

- 文本包含关键字
    - str:contains(content.text.text, 'Hello')

- 文本以某前缀开始 / 以某后缀结束
    - str:startWith(content.text.text, 'Notice')
    - str:endWith(content.text.text, '.pdf')

- 文本忽略大小写匹配
    - str:contains(str:toLowerCase(content.text.text), 'urgent')

- 正则匹配
    - re:isMatch('^Invoice\\s*#?\\d+', content.text.text)

消息元数据（id、chatId、date、sender 等）

- 按 id 或 chatId 过滤
    - id > 1000
    - chat_id == 1234567890

- 按发送者或方向
    - sender_user_id == 99999
    - is_outgoing == True

- 日期范围
    - date >= 1672531200 and date <= 1672617600

媒体类型与大小（通过 content 直接访问）

- 视频或文档大小过滤
    - content.video.video.size > 4000
    - content.document.document.size > 1048576

- 判断是否有 caption 或特定媒体属性
    - content.caption != null && str:contains(content.caption.text, 'Summary')

FileRecord（别名 f）常用方法写法

- 按文件大小与类型
    - f.size() > 1048576
    - f.type() == 'video' or f.type() == 'photo'

- 按文件名 / 后缀 / MIME
    - str:contains(f.fileName(), 'report')
    - str:endWith(f.fileName(), '.zip')
    - f.mimeType() == 'application/pdf'

参考文档

- [Python 表达式文档](https://docs.python.org/3/reference/expressions.html)
- [TdApi.Message 文档](https://core.telegram.org/tdlib/docs/classtd_1_1td__api_1_1message.html)
