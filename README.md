# 1. NJU-Data-Integration
2021 Spring NJUSE Data Integration

# 2. 数据获取
1. 静态数据获取：使用Hadoop + Sqoop + Beeline完成静态数据的获取，总共获取到135M的数据(csv格式)
2. 流数据获取：使用Kafka获取流数据，并先存储为txt格式文件，总共获取到52947000条记录，分析使用了4月11日0:27分到4月14日20:18分的数据，一共50216000条。

# 3. 元数据
本部分描述了静态数据和流数据的元数据信息

## 3.1. 静态数据元数据
```sql
create table buy_data
(
    id          bigint,       # id标识符，无含义
    user_id     int,          # user_id 用户id，和流数据userId对应
    item_id     int,          # item_id 物品id，和流数据ItemId对应
    category_id int,          # category_id 类别id，和流数据category_id对应
    type        varchar(32),  # 类型，均为buy
    `timestamp` int           # 操作的时间戳，UTC 秒
)
```

## 3.2. 流数据元数据

### 3.2.1. 浏览商品
| URL             | 参数                       | 额外参数        |
| --------------- | -------------------------- | --------------- |
| /item/getDetail | userId、itemId、categoryId | SESSIONID、time |

```json
{
  "userId" : "708001",
  "itemId" : "1073983",
  "categoryId" : "4145813"
}
```

### 3.2.2. 收藏商品
| URL         | 参数                       | 额外参数        |
| ----------- | -------------------------- | --------------- |
| /item/favor | userId、itemId、categoryId | SESSIONID、time |

```json
{
  "userId" : "846271",
  "itemId" : "1594140",
  "categoryId" : "983613"
}
```

### 3.2.3. 商品加入购物车
| URL        | 参数                       | 额外参数        |
| ---------- | -------------------------- | --------------- |
| /item/cart | userId、itemId、categoryId | SESSIONID、time |

```json
{
  "userId" : "588929",
  "itemId" : "5048272",
  "categoryId" : "1567637"
}
```

### 3.2.4. 购买商品
| URL       | 参数                                              | 额外参数        |
| --------- | ------------------------------------------------- | --------------- |
| /item/buy | userId、itemId、categoryId、isSecondKill、success | SESSIONID、time |

```json
{
  "userId" : "805004",
  "itemId" : "235951",
  "categoryId" : "3112817",
  "isSecondKill" : "0" // 表明这是一个秒杀请求，如果字段为1，则后面会跟一个额外字段success，表示秒杀是否成功，文档里有提到会有秒杀请求
}
```

### 3.2.5. 用户登录
| URL         | 参数                                | 额外参数          |
| ----------- | ----------------------------------- | ----------------- |
| /user/login | userId、password、authCode、success | IPADDR、SESSIONID |


```json
{
  "userId" : "816972",
  "password" : "3b5c41e94ca564cd54fceba81c9c16c4",
  "authCode" : "74900269f04eb3decbda5bd4de0c5a62",
  "success" : "1"
}
```

# 4. 数据处理

## 4.1. 流数据
1. 数据处理
   1. 将txt格式的数据处理成csv格式的数据
   2. 根据请求类型将数据进行划分
   3. 根据SESSIONID、userId和ipaddr进行划分
2. 机器人分析：补充：**用户在部分时间段上会出现恶意机器人行为，而并不是一直会有恶意机器人行为**
   1. 撞库机器人：使用ipaddr进行分类，检查如下，[详见](./Stream/extract/login_processor.py)
      1. 某个时间段多次尝试登陆不同用户
      2. 大量的登录错误，并且登录成功后操作少(和每个用户成功登录的中位数对比)
   2. 抢单机器人：对userId进行划分，检查如下，[详见](./Stream/extract/grab_processor.py)
      1. 购买时间是否集中在整点或半点
      2. 检查秒杀的次数和成功的情况(目前适合整个系统的均值比较 TODO 和每个用户的成功的中位数比较)
      3. 数据集位置：grab
   3. 刷单机器人：对userId进行划分，检查如下，[详见](./Stream/extract/swipe_processor.py)
      1. 是否某一个时间段购买行为集中在同一商品上，或者同一个时间购买大量非cart的商品
      2. 其他特征：平均浏览购买时间等
   4. 爬虫机器人：对userId进行划分，检查如下，[详见](./Stream/extract/spider_processor.py)
      1. 是否在某个时间段内大量顺序浏览单一或多个目录下的商品
      2. 检查浏览速度是不是远低于平均速度。
3. 分析目标
   1. 列出发现和判断出的恶意机器人
   2. 列出机器人的显著特征并指出他们在日志中的体现
4. 需要的物化表
   1. 用户物化表(用户id，秒杀成功次数，秒杀次数，正常购买次数，登录成功次数，登录次数，同一商品最大购买次数)
   2. IP地址物化表(IP地址，登录次数，登录成功次数，登录成功用户数)
   3. 爬虫机器人需要流式检查。
5. 静态数据需要的物化表
   1. 用户物化表(用户id，秒杀成功次数，秒杀次数，正常购买次数，平均购买前同一类别商品浏览次数，平均购买前浏览次数，平均购买前浏览时间，cart次数、favor次数，cart后购买次数、favor后购买次数、不同时间段的登录频率、成功登录IP地址数量)
   2. SESSIONID物化表(SESSIONID，用户id，getDetail次数，favor次数，cart次数，buy次数，login次数)
6. <a href = "https://box.nju.edu.cn/d/8ca4d3496b94418dba42/">中间数据的下载地址</a>
   1. format：从txt调整为csv格式的数据
   2. category：将csv格]式的数据转换为按照category进行划分。
   3. sessionId：将category的数据按照session_id进行排序的结果。
   4. userId：将category的数据按照user_id进行排序的结果。
   5. login：找到的所有的撞库机器人，认为有问题的结果存放在_result为后缀的文件中
   6. grab：找到的所有的抢单机器人，认为有问题的结果存放在_result为后缀的文件中
   7. swipe：找到所有的刷单机器人，认为有问题的结果存放在_result为后缀的文件中
   
## 4.2. 静态数据
1. 数据清洗
   1. 清洗掉所有在流数据被识别为机器人的相关数据
   2. 根据userId进行划分分类统计
   3. 静态数据的时间戳和流数据的时间是一致的
2. 数据处理，KPI：购买转化、用户留存等
   1. 将静态数据和流数据结合，对每个用户按照流数据中的请求类型进行划分
   2. 对每个用户的所有login, cart, getDetail, favor, buy等操作，按照time，Category为主维度，getDetail/buy, cart/buy, favor/buy, buy等多种频数或频度，给用户打上tag，对每一个用户进行多维度的分析。以购买行为转化率作为KPI，我们可以获得
      1. 用户的决策速度，浏览多少同一类别商品后会产生购买行为，浏览多久商品后会产生购买行为，平均浏览多少商品后产生购买行为
      2. 用户的购物偏好，是会选择在购物车里的商品，或是favor的商品，还是倾向于浏览后选择
      3. 用户的常用时间，对用户的常见操作进行统计，获得用户在不同时间段的登录频率和频数
      4. 用户下单次数和频度
   3. 根据用户的login信息，我们可以统计出用户的持有设备数量，进而对用户购买力进行一定的划分或是相关性调查
3. 数据分析：
   1. 获取计算得到不同用户的购买转化率
   2. 按照用户的不同维度信息，进行聚类分析，划分类群
   3. 对全局和不同类群中的用户分别统计，获得用户属性和偏好分布
   4. 获取到用户的购买偏好、下单次数、频数、浏览时间倾向
4. 其他可供参考的思路
   1. 基于区段的机器人流量的分段分析
   2. 捆绑销售(部分商品可以进行捆绑销售，使用KNN或者aprioir算法)
   3. 用户画像(为每一个用户绘制特定的用户画像)
   4. 分析商品的复购率和热门产品
   
```
0.004
      support            itemsets  length
110  0.004596   (140410, 1464116)       2
111  0.004434   (140410, 2735466)       2
112  0.004236   (2885642, 140410)       2
113  0.004262   (140410, 4145813)       2
114  0.004207  (4159072, 2885642)       2

0.003
      support            itemsets  length
154  0.003349    (140410, 982926)       2
155  0.004596   (140410, 1464116)       2
156  0.004434   (140410, 2735466)       2
157  0.004236   (2885642, 140410)       2
158  0.004262   (140410, 4145813)       2
159  0.003704   (4756105, 140410)       2
160  0.003490   (4801426, 140410)       2
161  0.003132   (2735466, 982926)       2
162  0.003077   (4756105, 982926)       2
163  0.003284   (4801426, 982926)       2
164  0.003058  (2735466, 1464116)       2
165  0.003787  (2735466, 4145813)       2
166  0.003077  (4756105, 2735466)       2
167  0.003086  (4801426, 2735466)       2
168  0.004207  (4159072, 2885642)       2
169  0.003241  (4756105, 4145813)       2
```

# 5. 数据可视化
1. Superset
   1. Superset是apache下的可视化平台
   2. 可以直接连接hive进行可视化，也可以导入csv，简化了数据之间的流通。
   3. 安装配置过程较为简单
2. 使用PowerBI
   1. 是微软旗下的BI平台，可连接到任何数据并对数据进行可视化
   2. 支持多种操作，并且拥有良好的人机交互。
   3. 交互式数据可视化效果创建不同的报表
3. 使用Tableau
   1. 一款方便且强大的数据可视化工具
   2. 处理海量数据时输入很不方便
4. 使用Matplotlib
   1. Python可视化库
   2. 配合Python程序使用方便、效果直观
   3. 图片可以用HTML、JPG、PNG等格式存储
5. 腾讯云图、DataV：大屏展示效果很好


