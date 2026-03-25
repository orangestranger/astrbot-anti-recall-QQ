# astrbot_plugin_anti_revoke

`AstrBot + NapCatQQ` 的 QQ 防撤回插件，仓库内容与线上插件目录 `astrbot_plugin_anti_revoke` 对齐。（修复了语音和转发消息无法显示问题）

当前支持：

- 文本
- 图片
- 语音
- 文件
- 合并转发

## 目录对应关系

这个仓库根目录就是插件目录内容，部署时放到：

```text
/AstrBot/data/plugins/astrbot_plugin_anti_revoke
```

目录内主要文件：

- `main.py`: 插件主逻辑
- `_conf_schema.json`: AstrBot 后台配置项
- `metadata.yaml`: 插件元数据
- `README.md`: 安装与使用说明

## 配置项

- `monitor_groups`: 要监听防撤回的群号列表
- `target_receivers`: 接收撤回提醒的 QQ 列表
- `target_groups`: 接收撤回提醒的群列表
- `ignore_senders`: 忽略这些发送者的撤回消息
- `cache_expiration_time`: 消息缓存时间，单位秒
- `file_size_threshold_mb`: 文件和视频缓存大小上限，单位 MB

示例配置：

```json
{
  "monitor_groups": ["123456789"],
  "target_receivers": ["10001"],
  "target_groups": ["987654321"],
  "ignore_senders": [],
  "cache_expiration_time": 300,
  "file_size_threshold_mb": 300
}
```

上面的效果是：

- 只监听群 `123456789`
- 撤回后私发给 QQ `10001`
- 同时转发到群 `987654321`

## 安装步骤

1. 把仓库内容放到 `/AstrBot/data/plugins/astrbot_plugin_anti_revoke`
2. 在 AstrBot 后台启用插件
3. 按需填写 `monitor_groups`、`target_receivers`、`target_groups`
4. 重载插件或重启 AstrBot

## 已处理的兼容点

- 语音优先使用 `url` 下载缓存，`get_file` 作为回退
- 合并转发优先缓存 `get_forward_msg` 返回的节点数据
- 合并转发重发时会把节点转换为 NapCat 可接受的 `node` 结构

## 建议测试

部署后建议至少测试这几种场景：

1. 纯文字撤回
2. 图片加文字撤回
3. 纯语音撤回
4. 合并转发撤回
5. 文本加合并转发撤回
