# mysql_flashback
MySQL Binlog 闪回工具，模拟成 Slave 连入 Master，读取 Binlog 生成重做或回滚SQL

## Use cases
* 生成重做SQL
* 使用 -r(--reverse) 选项生成回滚SQL，在数据库误操作后，及时恢复数据

## Requirements
* Python 2.7
* 依赖于[python-mysql-replication](https://github.com/noplay/python-mysql-replication)：`pip install mysql-replication`
* MySQL 5.6 或 MariaDB 10.0，其它版本未进行测试
* MySQL必须处于启动状态，暂不支持直接从Binlog文件生成SQL(TODO)
* Binlog 处于开启状态，并且是 row 格式

## Examples
> python flashback.py --help
>
> python flashback.py -H localhost -u root -p 123456 -o output.sql -t "test.*" -r


对于以下SQL操作：
``` sql
DROP TABLE IF EXISTS test.test_flashback;

CREATE TABLE test.test_flashback(
    c_tinyint TINYINT NOT NULL,
    c_int INT NOT NULL,
    c_bigint BIGINT NOT NULL,
    c_decimal DECIMAL(9,3) NOT NULL,
    c_varchar VARCHAR(20) NOT NULL,
    c_varbinary VARBINARY(20) NOT NULL,
    c_text TEXT NOT NULL,
    c_date DATE NOT NULL,
    c_time TIME NOT NULL,
    c_datetime DATETIME NOT NULL,
    c_timestamp TIMESTAMP NULL,
    c_null INT NULL,
    PRIMARY KEY (c_int)
);

INSERT INTO test.test_flashback (c_tinyint, c_int, c_bigint, c_decimal, c_varchar, c_varbinary, c_text, c_date, c_time, c_datetime, c_timestamp)
VALUES (10, 123456789, 3000000000, '8888.444', '我是varchar\n第二行', 0x686868, '我是text', '2017-01-17', '11:11', '2017-01-17 11:11', '2017-01-17 11:11');

UPDATE test.test_flashback SET
    c_tinyint = 10, c_int = 9999, c_bigint = 2222, c_decimal = 666.999,
    c_varchar = '新的varchar', c_varbinary = 0xa1a1a1, c_text = '新的text\n第二行',
    c_date = '2017-02-03', c_time = '23:59:59', c_datetime = '2030-01-01 10:50', c_timestamp = NULL, c_null = 777
WHERE c_int = 123456789;

DELETE FROM test.test_flashback WHERE c_int = 9999;
```

使用 -r 选项生成的回滚SQL如下：

```sql
-- ==## 2017-01-25 16:49:31 bin.000001-1795 DeleteRowsEvent rows:1
INSERT INTO `test`.`test_flashback`(`c_tinyint`,`c_int`,`c_bigint`,`c_decimal`,`c_varchar`,`c_varbinary`,`c_text`,`c_date`,`c_time`,`c_datetime`,`c_timestamp`,`c_null`) VALUES
(10, 9999, 2222, '666.999', '新的varchar', 0xa1a1a1, '新的text
第二行', '2017-02-03', '23:59:59', '2030-01-01 10:50:00', Null, 777);

-- ==## 2017-01-25 16:49:31 bin.000001-1550 UpdateRowsEvent rows:1
UPDATE `test`.`test_flashback`
SET `c_tinyint` = 10
   ,`c_int` = 123456789
   ,`c_bigint` = 3000000000
   ,`c_decimal` = '8888.444'
   ,`c_varchar` = '我是varchar
第二行'
   ,`c_varbinary` = 0x686868
   ,`c_text` = '我是text'
   ,`c_date` = '2017-01-17'
   ,`c_time` = '11:11:00'
   ,`c_datetime` = '2017-01-17 11:11:00'
   ,`c_timestamp` = '2017-01-17 11:11:00'
   ,`c_null` = Null
WHERE `c_tinyint` = 10
  AND `c_int` = 9999
  AND `c_bigint` = 2222
  AND `c_decimal` = '666.999'
  AND `c_varchar` = '新的varchar'
  AND `c_varbinary` = 0xa1a1a1
  AND `c_text` = '新的text
第二行'
  AND `c_date` = '2017-02-03'
  AND `c_time` = '23:59:59'
  AND `c_datetime` = '2030-01-01 10:50:00'
  AND `c_timestamp` is Null
  AND `c_null` = 777
LIMIT 1;

-- ==## 2017-01-25 16:49:30 bin.000001-1225 WriteRowsEvent rows:1
DELETE FROM `test`.`test_flashback`
WHERE `c_tinyint` = 10
  AND `c_int` = 123456789
  AND `c_bigint` = 3000000000
  AND `c_decimal` = '8888.444'
  AND `c_varchar` = '我是varchar
第二行'
  AND `c_varbinary` = 0x686868
  AND `c_text` = '我是text'
  AND `c_date` = '2017-01-17'
  AND `c_time` = '11:11:00'
  AND `c_datetime` = '2017-01-17 11:11:00'
  AND `c_timestamp` = '2017-01-17 11:11:00'
  AND `c_null` is Null
LIMIT 1;


-- ==## 2017-01-25 16:49:39.393000 completed OK!
```
