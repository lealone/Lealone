## 用 MongoDB 客户端访问 Lealone

执行以下命令启动 MongoDB 客户端:

`mongosh mongodb://127.0.0.1:27017/mydb?serverSelectionTimeoutMS=200000`

```sql
Connecting to:          mongodb://127.0.0.1:27017/mydb?serverSelectionTimeoutMS=200000
Using MongoDB:          6.0.0
Using Mongosh:          1.9.1

For mongosh info see: https://docs.mongodb.com/mongodb-shell/

mydb> db.runCommand({ insert: "c1", documents: [{ _id: 1, user: "u1", status: "A"}] });
{ ok: 1, n: 1 }
mydb>

mydb> db.runCommand({ find: "c1" });
{
  cursor: {
    id: Long("0"),
    ns: 'mydb.c1',
    firstBatch: [
      { _id: 1, user: 'u1', status: 'A' },
      { _id: 1, user: 'u1', status: 'A' }
    ]
  },
  ok: 1
}

mydb>
```

## 在 IDE 中测试 CRUD 操作

执行 [MongoCrudTest](https://github.com/lealone/Lealone/blob/master/lealone-test/src/test/java/org/lealone/test/mongo/MongoCrudTest.java)

