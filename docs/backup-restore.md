# 备份与恢复

## PostgreSQL

建议每日全量备份并持续归档 WAL，以实现时间点恢复：

```bash
pg_dump --format=custom --file=practice.dump "$DATABASE_URL"
pg_restore --clean --if-exists --dbname="$DATABASE_URL" practice.dump
```

生产环境应加密备份文件，并将访问权限限制为数据库运维角色。恢复完成后至少核对：

- 用户数、练习数和音频记录数。
- 对象存储中的对象数与 `media_assets` 记录数。
- 抽查完成的练习、标记、目标和统计结果。
- Prisma 迁移历史与代码期望一致。

## 对象存储

启用 Bucket 版本控制或按环境定义生命周期策略。不要将音频对象公开。备份或复制策略必须保留对象 Key，恢复后 `object_key` 才能继续解析。

## Redis

Redis 只保存短期队列和心跳，不是业务数据源。可从 PostgreSQL 重新生成清理/探测任务；不要将 Redis 备份作为业务恢复依据。

## 恢复演练

每月至少执行一次恢复演练：

1. 在隔离环境恢复 PostgreSQL。
2. 恢复或挂载对象存储快照。
3. 启动 API 和 Worker，执行健康检查。
4. 抽查历史详情、播放 URL、标记、目标和统计。
5. 记录恢复耗时、数据差异和后续修正项。

## 删除策略

练习删除进入 `DELETING`，Worker 在单个数据库事务内对涉及的每个 `media_objects` 登记行加行锁、级联删除练习及其 `media_assets` 引用，并把删除后零引用的对象登记行置为 `DELETE_PENDING`；随后由可重试的清扫任务再次复核引用数后删除 S3 对象和登记行。

两个练习复用同一音频时即使并发删除，行锁也会让引用计数串行可见，最后一个删除者必然墓碑化对象，不会遗留 S3 孤儿；只要还有其他练习引用，对象绝不会被误删。物理对象删除幂等（对象已消失也视为成功），S3 暂时不可用时墓碑保留并记录 `delete_attempts/last_delete_error`，由清扫任务（清理后触发、Worker 启动时、每 5 分钟定时）持续重试。练习记录删除最终失败时状态为 `DELETE_FAILED`，应告警并重试，不得静默忽略。
