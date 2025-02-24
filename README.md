# mysql2excel

mysql2excel 是一个允许您将数据从 MySQL 数据库比对并导出到 Excel 文件的工具。

## 打包

```bash
go build mysql2excel
```

## 用法

```bash
mysql2excel dump --source user:password@host:port --db db1
mysql2excel diff --source user:password@host:port --target user:password@host:port --db db1:db2
```
