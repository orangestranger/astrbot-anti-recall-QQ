# astrbot-anti-recall-QQ

AstrBot QQ 防撤回插件，支持：

- 文本
- 图片
- 语音
- 文件
- 合并转发

## 配置

- `monitor_groups`: 监听的群号列表
- `target_receivers`: 接收撤回消息的 QQ 列表
- `target_groups`: 接收撤回消息的群列表
- `ignore_senders`: 忽略的发送者 QQ 列表
- `cache_expiration_time`: 缓存秒数
- `file_size_threshold_mb`: 文件和视频缓存大小限制

## 安装

将仓库内容放入 AstrBot 插件目录，例如：

```text
/AstrBot/data/plugins/astrbot_plugin_anti_revoke
```

然后在 AstrBot 后台启用插件并填写配置。
