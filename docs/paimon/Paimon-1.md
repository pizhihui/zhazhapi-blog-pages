
---
outline: deep
---

实时数仓：

paimon Paimon Bundled Jar 下载地址：https://repository.apache.org/content/groups/snapshots/org/apache/paimon/paimon flink-1.18/0.8-SNAPSHOT/paimon-flink-1.18-0.8-20240301.002155-30.jar 

Hadoop Bundled Jar https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.7.5-9.0/flinkshaded-hadoop-2-uber-2.7.5-9.0.jar


# 安装

各软件版本：

- Flink：1.17.2
- FlinkCDC：2.4.2
- Paimon：0.8.1
- MySQL server：5.7
- MySQL connector：8.0.x

详细安装过程



# 数据分类

## 点击流数据

主要来源于 kafka


## 业务数据

主要来源于 MySQL，使用 Flink CDC 工具来同步


## 阿里云案例数据

参考连接：[基于Flink+Paimon搭建流式湖仓_实时计算 Flink版(Flink)-阿里云帮助中心 (aliyun.com)](https://help.aliyun.com/zh/flink/use-cases/build-a-streaming-data-warehouse-based-on-flink-and-apache-paimon?spm=a2c4g.11186623.0.0.77ae9529OzOuCt#1b45d9e082tel)


MySQL 表

## t_orders

## t_orders_pay
## t_product_catalog

```sql

-- 创建库  
CREATE DATABASE IF NOT EXISTS 'gmall_tiny_db';  

-- 创建 t_orders 表
CREATE TABLE `t_orders` (  
  f_order_id BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键字段',  
  f_user_id VARCHAR(50) NOT NULL COMMENT '用户 id',  
  f_shop_id BIGINT(20) NOT NULL COMMENT '店铺 id',  
  f_product_id BIGINT(20) NOT NULL COMMENT '商品 id',  
  f_buy_fee BIGINT(20) NOT NULL COMMENT '购买金额',  
  f_create_time TIMESTAMP NOT NULL COMMENT '创建时间',  
  f_update_time timestamp NOT NULL DEFAULT now() COMMENT '修改时间',  
  f_state INT(5) NOT NULL COMMENT '状态',  
  PRIMARY KEY (`f_order_id`) USING BTREE  
) ENGINE=InnoDB CHARACTER SET=utf8 COMMENT='订单表';  
  
  
CREATE TABLE `t_orders_pay` (  
  f_pay_id BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键字段',  
  f_order_id BIGINT(20) NOT NULL COMMENT '订单 id',  
  f_pay_platform INT(5) NOT NULL COMMENT '支付平台,0-phone|1-pc',  
  f_create_time TIMESTAMP NOT NULL COMMENT '创建时间',  
  PRIMARY KEY (`f_pay_id`) USING BTREE  
) ENGINE=InnoDB CHARACTER SET=utf8 COMMENT='订单支付表';  
  
  
CREATE TABLE `t_product_catalog` (  
  f_product_id BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键字段',  
  f_catalog_name VARCHAR(50) NOT NULL,  
  PRIMARY KEY (`f_product_id`) USING BTREE  
) ENGINE=InnoDB CHARACTER SET=utf8 COMMENT='商品表';  
  
-- 准备数据  
INSERT INTO orders VALUES  
(100001, 'user_001', 12345, 1, 5000, '2023-02-15 16:40:56', '2023-02-15 18:42:56', 1),  
(100002, 'user_002', 12346, 2, 4000, '2023-02-15 15:40:56', '2023-02-15 18:42:56', 1),  
(100003, 'user_003', 12347, 3, 3000, '2023-02-15 14:40:56', '2023-02-15 18:42:56', 1),  
(100004, 'user_001', 12347, 4, 2000, '2023-02-15 13:40:56', '2023-02-15 18:42:56', 1),  
(100005, 'user_002', 12348, 5, 1000, '2023-02-15 12:40:56', '2023-02-15 18:42:56', 1),  
(100006, 'user_001', 12348, 1, 1000, '2023-02-15 11:40:56', '2023-02-15 18:42:56', 1),  
(100007, 'user_003', 12347, 4, 2000, '2023-02-15 10:40:56', '2023-02-15 18:42:56', 1)  
;  
  
INSERT INTO t_orders_pay VALUES  
(2001, 100001, 1, '2023-02-15 17:40:56'),  
(2002, 100002, 1, '2023-02-15 17:40:56'),  
(2003, 100003, 0, '2023-02-15 17:40:56'),  
(2004, 100004, 0, '2023-02-15 17:40:56'),  
(2005, 100005, 0, '2023-02-15 18:40:56'),  
(2006, 100006, 0, '2023-02-15 18:40:56'),  
(2007, 100007, 0, '2023-02-15 18:40:56')  
;  
  
  
INSERT INTO t_product_catalog VALUES  
(1, 'phone_aaa'),  
(2, 'phone_bbb'),  
(3, 'phone_ccc'),  
(4, 'phone_ddd'),  
(5, 'phone_eee')  
;
```


# 总体思路

需要考虑的几个问题：

1. catalog 如何管理
2. 各层的计算如何选择
3. 应对分析、AI 场景

下面详细来分析各个点改如何考虑

## Catalog 选择

如果公司已经数仓，比如基于 hadoop 的数仓，那么基本都是 hdfs 存储 + Hive metastore + Spark 计算的模式来构建的，那么首先基于湖仓的技术肯定不能全部放弃，只能基于原先的数仓升级。

那么这里很有可能离线数仓的数据需要灌到湖仓里面来，并且为了统一管理元数据，这里选择 Hive catalog

## 计算引擎选择

毫无疑问：那么肯定是 Flink + Paimon 来构建每一层的数据


## 分析场景

这里可能是基于 Spark，或者基于 Presto 来构建


# Catalog

## JDBC Catalog

Paimon 可以使用 Hive 或这 JDBC 或者 Memory 的 Catalog，这里暂且使用 MySQL 的 Catalog

```sql
CREATE CATALOG my_jdbc_dw WITH (
    'type' = 'paimon',
    'metastore' = 'jdbc',
    'uri' = 'jdbc:mysql://192.168.31.57:3306/paimon_dw_catalog_db',
    'jdbc.user' = 'root', 
    'jdbc.password' = '123456', 
    'catalog-key'='jdbc',
    'warehouse' = 'hdfs:///paimon-dw',
    'lock-key-max-length' = '512'
);

USE CATALOG my_jdbc;
```


注意：`lock-key-max-length`的配置跟 lock 的名称规则有关系，规则：{catalog-key}.{database-name}.{table-name}


## Hive Catalog


```sql
CREATE CATALOG paimon_hive_dw WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://thrift://1.hadoop.com:9083' -- default use 'hive.metastore.uris' in HiveConf
    'hive-conf-dir' = '/opt/app/', 
    'hadoop-conf-dir' = '/opt/app/', 
    'warehouse' = 'hdfs:///paimon-dw' -- default use 'hive.metastore.warehouse.dir' in HiveConf
);

USE CATALOG paimon_hive_dw;
```



# ODS 层（同步 MySQL 数据）

这里区分，如果是数据都在一个实例上面，可以采用整库同步来全面来同步

但是如果是实际生产中，特别是业务经历过很长时间，例如订单是分库分表的，用户表也是分库分表的，这里就建议还是分开，按单独的表采集

## 整库同步

官网链接：[Mysql CDC | Apache Paimon](https://paimon.apache.org/docs/0.8/flink/cdc-ingestion/mysql-cdc/#synchronizing-databases)

直接使用整库同步的工具来同步

```bash

./bin/flink run -m yarn-cluster -p 3 -ytm 2048 -yjm 2048 -yqu root.queue_realtime\
    -ynm paimon-sync-database-test \
    -Dexecution.checkpointing.interval=10s \
    -Dtaskmanager.numberOfTasksSlots=1 \
    ./paimon-flink-action-0.8.1.jar \
    mysql_sync_database
    --warehouse hdfs:///paimon-dw  \
    --database gmall2024_ods \
    --ignore_incompatible true \
    --merge_shards true/false \
    --table_prefix ods_ \
    --table_suffix 2024 \
    --mode divided \
    --mysql-conf hostname=192.168.31.56 \
    --mysql-conf port=3306 \
    --mysql-conf username=root \
    --mysql-conf password=123456 \
    --mysql_conf database-name=gmall2024_db \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://1.hadoop.com:9083 \
    --table-conf bucket=2 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

其他配置：
```properties
    [--including_tables <mysql-table-name|name-regular-expr>] 
    [--excluding_tables <mysql-table-name|name-regular-expr>] 
	[--metadata_column <metadata-column>] 
    [--type_mapping <option1,option2...>] 
```

注意：mode 的配置，
- "divided" (the default mode if you haven't specified one): start a sink for each table, the synchronization of the new table requires restarting the job.
- "combined": start a single combined sink for all tables, the new table will be automatically synchronized.


如果采用 hive metastore 的配置

```bash
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://1.hadoop.com:9083 \
```


注意：这里分区字段是咋指定的了？


## 表同步

注意：这里



# DIM 层（TODO）





# DWD 层

## my_jdbc_dw.order_dw.dwd_orders

```sql

CREATE TABLE my_jdbc_dw.order_dw.dwd_orders (  
    order_id BIGINT,  
    order_user_id STRING,  
    order_shop_id BIGINT,  
    order_product_id BIGINT,  
    order_product_catalog_name STRING,  
    order_fee BIGINT,  
    order_create_time TIMESTAMP,  
    order_update_time TIMESTAMP,  
    order_state INT,  
    pay_id BIGINT,  
    pay_platform INT COMMENT 'platform 0: phone, 1: pc',  
    pay_create_time TIMESTAMP,  
    PRIMARY KEY (order_id) NOT ENFORCED  
) WITH (  
    'merge-engine' = 'partial-update', -- 使用部分更新数据合并机制产生宽表  
    'changelog-producer' = 'lookup' -- 使用lookup增量数据产生机制以低延时产出变更数据  
);

```

```sql
SET 'execution.checkpointing.max-concurrent-checkpoints' = '3';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '10s';

-- Paimon目前暂不支持在同一个作业里通过多条INSERT语句写入同一张表，因此这里使用UNION ALL。
INSERT INTO dw.order_dw.dwd_orders 
SELECT 
    o.order_id,
    o.user_id,
    o.shop_id,
    o.product_id,
    dim.catalog_name,
    o.buy_fee,
    o.create_time,
    o.update_time,
    o.state,
    NULL,
    NULL,
    NULL
FROM
    dw.order_dw.orders o 
    LEFT JOIN dw.order_dw.product_catalog FOR SYSTEM_TIME AS OF proctime() AS dim
    ON o.product_id = dim.product_id
UNION ALL
SELECT
    order_id,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    pay_id,
    pay_platform,
    create_time
FROM
    dw.order_dw.orders_pay;
```



查看数据：

```sql
SELECT * FROM dw.order_dw.dwd_orders ORDER BY order_id;
```


# DWS 层


```sql
-- 用户维度聚合指标表。
CREATE TABLE my_jdbc_dw.order_dw.dws_users (
    user_id STRING,
    ds STRING,
    payed_buy_fee_sum BIGINT COMMENT '当日完成支付的总金额',
    PRIMARY KEY (user_id, ds) NOT ENFORCED
) WITH (
    'merge-engine' = 'aggregation', -- 使用预聚合数据合并机制产生聚合表
    'fields.payed_buy_fee_sum.aggregate-function' = 'sum' -- 对 payed_buy_fee_sum 的数据求和产生聚合结果
    -- 由于dws_users表不再被下游流式消费，因此无需指定增量数据产生机制
);

-- 商户维度聚合指标表。
CREATE TABLE my_jdbc_dw.order_dw.dws_shops (
    shop_id BIGINT,
    ds STRING,
    payed_buy_fee_sum BIGINT COMMENT '当日完成支付总金额',
    uv BIGINT COMMENT '当日不同购买用户总人数',
    pv BIGINT COMMENT '当日购买用户总人次',
    PRIMARY KEY (shop_id, ds) NOT ENFORCED
) WITH (
    'merge-engine' = 'aggregation', -- 使用预聚合数据合并机制产生聚合表
    'fields.payed_buy_fee_sum.aggregate-function' = 'sum', -- 对 payed_buy_fee_sum 的数据求和产生聚合结果
    'fields.uv.aggregate-function' = 'sum', -- 对 uv 的数据求和产生聚合结果
    'fields.pv.aggregate-function' = 'sum' -- 对 pv 的数据求和产生聚合结果
    -- 由于dws_shops表不再被下游流式消费，因此无需指定增量数据产生机制
);

-- 为了同时计算用户视角的聚合表以及商户视角的聚合表，另外创建一个以用户 + 商户为主键的中间表。
CREATE TABLE my_jdbc_dw.order_dw.dwm_users_shops (
    user_id STRING,
    shop_id BIGINT,
    ds STRING,
    payed_buy_fee_sum BIGINT COMMENT '当日用户在商户完成支付的总金额',
    pv BIGINT COMMENT '当日用户在商户购买的次数',
    PRIMARY KEY (user_id, shop_id, ds) NOT ENFORCED
) WITH (
    'merge-engine' = 'aggregation', -- 使用预聚合数据合并机制产生聚合表
    'fields.payed_buy_fee_sum.aggregate-function' = 'sum', -- 对 payed_buy_fee_sum 的数据求和产生聚合结果
    'fields.pv.aggregate-function' = 'sum', -- 对 pv 的数据求和产生聚合结果
    'changelog-producer' = 'lookup', -- 使用lookup增量数据产生机制以低延时产出变更数据
    -- dwm层的中间表一般不直接提供上层应用查询，因此可以针对写入性能进行优化。
    'file.format' = 'avro', -- 使用avro行存格式的写入性能更加高效。
    'metadata.stats-mode' = 'none' -- 放弃统计信息会增加OLAP查询代价（对持续的流处理无影响），但会让写入性能更加高效。
);
```


## my_jdbc_dw.order_dw.dwm_users_shops

```sql
SET 'execution.checkpointing.max-concurrent-checkpoints' = '3';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '10s';

INSERT INTO my_jdbc_dw.order_dw.dwm_users_shops
SELECT
    order_user_id,
    order_shop_id,
    DATE_FORMAT (pay_create_time, 'yyyyMMdd') as ds,
    order_fee,
    1 -- 一条输入记录代表一次消费
FROM my_jdbc_dw.order_dw.dwd_orders
WHERE pay_id IS NOT NULL AND order_fee IS NOT NULL;
```

## my_jdbc_dw.order_dw.dws_users

## my_jdbc_dw.order_dw.dws_shops

```sql
SET 'execution.checkpointing.max-concurrent-checkpoints' = '3';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '10s';

-- 与dwd不同，此处每一条INSERT语句写入的是不同的Paimon表，可以放在同一个作业中。
BEGIN STATEMENT SET;

INSERT INTO my_jdbc_dw.order_dw.dws_users
SELECT 
    user_id,
    ds,
    payed_buy_fee_sum
FROM my_jdbc_dw.order_dw.dwm_users_shops;

-- 以商户为主键，部分热门商户的数据量可能远高于其他商户。
-- 因此使用local merge在写入Paimon之前先在内存中进行预聚合，缓解数据倾斜问题。
INSERT INTO my_jdbc_dw.order_dw.dws_shops /*+ OPTIONS('local-merge-buffer-size' = '64mb') */
SELECT
    shop_id,
    ds,
    payed_buy_fee_sum,
    1, -- 一条输入记录代表一名用户在该商户的所有消费
    pv
FROM my_jdbc_dw.order_dw.dwm_users_shops;

END;
```


```sql
--查看dws_users表数据
SELECT * FROM dw.order_dw.dws_users ORDER BY user_id;

--查看dws_shops表数据
SELECT * FROM dw.order_dw.dws_shops ORDER BY shop_id;
```


![[Pasted image 20240703193433.png]]



## 修改 MySQL 的数据

```sql
INSERT INTO orders VALUES
(100008, 'user_001', 12345, 3, 3000, '2023-02-15 17:40:56', '2023-02-15 18:42:56', 1),
(100009, 'user_002', 12348, 4, 1000, '2023-02-15 18:40:56', '2023-02-15 19:42:56', 1),
(100010, 'user_003', 12348, 2, 2000, '2023-02-15 19:40:56', '2023-02-15 20:42:56', 1);

INSERT INTO orders_pay VALUES
(2008, 100008, 1, '2023-02-15 18:40:56'),
(2009, 100009, 1, '2023-02-15 19:40:56'),
(2010, 100010, 0, '2023-02-15 20:40:56');
```


再次查询数据：
```sql
SELECT * FROM dw.order_dw.dws_users ORDER BY user_id;
```


![[Pasted image 20240703193522.png]]



```sql
SELECT * FROM dw.order_dw.dws_shops ORDER BY shop_id;
```

![[Pasted image 20240703193534.png]]


# 外部查询引擎

使用外部的 catalog 来查询

## 需求：查询23年2月15日交易额前三高的商户(from dws_shops)

```sql
SET odps.sql.common.table.planner.ext.hive.bridge = true;
SET odps.sql.hive.compatible = true;

SELECT ROW_NUMBER() OVER (ORDER BY payed_buy_fee_sum DESC) AS rn, 
       shop_id, 
       payed_buy_fee_sum 
FROM dws_shops
WHERE ds = '20230215'
ORDER BY rn LIMIT 3;
```

## 需求：查询某个客户23年2月特定支付平台支付的订单明细（from dwd_orders）

```sql
SET odps.sql.common.table.planner.ext.hive.bridge = true;
SET odps.sql.hive.compatible = true;

SELECT * 
FROM dwd_orders
WHERE 
	order_create_time >= '2023-02-01 00:00:00' 
		AND order_create_time < '2023-03-01 00:00:00'
		AND order_user_id = 'user_001'
		AND pay_platform = 0  -- 手机支付平台
	ORDER BY order_create_time;
```


## 需求：查询23年2月内每个品类的订单总量和订单总金额(from dwd_orders)

```sql
SET odps.sql.common.table.planner.ext.hive.bridge = true;
SET odps.sql.hive.compatible = true;

SELECT
  TO_CHAR(order_create_time, 'YYYYMMDD') AS order_create_date,
  order_product_catalog_name,
  COUNT(*),
  SUM(order_fee)
FROM
  dwd_orders
WHERE
	  order_create_time >= '2023-02-01 00:00:00'  
  and order_create_time < '2023-03-01 00:00:00'
GROUP BY
  order_create_date, order_product_catalog_name
ORDER BY
  order_create_date, order_product_catalog_name;
```


