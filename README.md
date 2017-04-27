# 商品数据聚类项目

## 目的

在商品数据上进行聚类算法的目的是通过机器学习的方法，自动将商品划分为不同类别，从而在进行价格分析等涉及商品分类的分析时，能够提供比按照商品名称（g_name）和规格型号（g_model）进行简单的 group by 更科学的商品分类依据。

## 聚类原理

本项目依照海关数据的特性以及数据量产生的分布式计算需求，采用了层次聚类算法。层次聚类的原理是在每一步运算时，从数据集中选取出相似度最高的两组数据，将它们合并为一组。之后不断重复该合并步骤直至到达一定的条件。算法完成后，每一个类内的数据为相似度较高的商品，同时类和类间的商品数据相似度较低。

## 项目简介

本项目底层选择了 hadoop 分布式计算框架，代码的具体实现将聚类的整体流程分解为多个步骤，通过一个或多个 MapReduce Job 实现每一步的计算。聚类流程共分为以下几步：

1. 初始化：

   对商品数据进行格式转换、分词、同义词替换等清洗工作。具体用例可见 someone_else.InitDemos 类。

2. 预聚类（去重）：

   通过局部敏感哈希算法将相似度极高的商品数据预先聚合成一类，之后每类仅取出一条数据代表本类参与后续计算。这样一方面由于一个类中都是相似度极高的数据，后续计算中不会出现误聚类的现象；另一方面减少了参与聚类的数据量，可以极大降低后续的计算代价。由于局部敏感哈希在短文本上的局限性，该算法只能用于聚合近似重复的数据，不适合直接用于聚类。具体用例可见 someone_else.SimHashDemos 类。

3. 计算单词权重：

   将商品数据的每一个单词通过 [TF - IDF 算法](https://zh.wikipedia.org/wiki/Tf-idf) 计算出它们相应的权值，使得商品数据从文本数据的形式转化成向量形式。具体用例可见 someone_else.TF_IDF_Demos 类。

4. 计算带权值的倒排索引

   通过[倒排索引](https://zh.wikipedia.org/zh-hans/%E5%80%92%E6%8E%92%E7%B4%A2%E5%BC%95)的方式表示整个数据空间，同时将单词的权值信息包含在倒排索引中。具体用例可见 someone_else.InvertedIndexDemos 类。

5. 通过倒排索引计算相似度矩阵

   要进行聚类，需要首先计算出数据集的相似度矩阵。本项目通过对倒排索引进行分割实现相似度矩阵的分布式计算。具体用例可见 someone_else.SimilarityDemos 类。

6. 通过相似度矩阵进行聚类

   本项目依照分治的思想，通过多层Kruscal算法实现层次聚类的分布式计算。具体用例可见 

7. 将聚类结果与原始商品数据进行关联

   聚类算法将商品按照生成的id划分成不同的类别。要在后续工作中使用该分类信息，还需将聚类的结果与原始商品数据相关联。具体用例可见



## 使用方法

环境配置：

* 使用本项目提供的 Hadoop，版本为 3.0.0-alpha2。
* 若使用其他 Hadoop，需要确定配置好 native library，同时确定 native library 中编译了 g-lib 。此外，不同版本的 Hadoop 配置文件不同，可以参考本项目的配置文件，位置在\{HADOOP_HOME}/etc/hadoop 中，需配置 hadoop-env.sh, core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml 五个文件，以及按照集群环境配置 slaves 文件。

1. 初始化：

   读入的文件格式可以是 csv 或 tsv，或任何其它纯文本格式。当文件格式不是 csv 或 tsv 时，需要初始化 job 时设置分隔符。文件内容形式为：

   ```
   entry_id, g_no, code_ts, g_name, g_model, [其他需要统计的数据]
   ```

   对应的Driver类用法：

   ```bash
   hadoop jar jar_name.jar clustering.init.Driver ${input_file} ${output_file}
   ```

   **参数说明：**

   * \${input_file} 输入文件在 HDFS 中路径，不包括文件名（后同）
   * \${output_file} 输出文件在 HDFS 中路径

2. 预聚类（去重）：

   读入经过初始化处理的数据，如果有特殊需要，设置 SimHash 的相似度阈值（不设置默认为 3）。对应 Driver 类用法：

   ```bash
   hadoop jar clustering.simhash.Driver ${input_file} ${output_file} [SimHash_threshold]
   ```

   **参数说明：**

   * \${input_file} 输入文件（初始化步骤的输出）在 HDFS 中路径
   * \${output_file} 输出文件在 HDFS 中路径
   * SimHash_threshold 判断两条 SimHash 签名是否相似的海明距离阈值，默认为 3

   *注意事项：*

   实际输出文件保存在 \${output_file}/result 中，中间结果保存在 \${output_file}/step1 中，最后的关联步骤会用到。

3. 计算单词权重：

   读入去重后的数据，分别计算每条商品记录中每个单词的权重。对应 Driver 类用法：

   ```bash
   hadoop jar clustering.tf_idf.Driver ${input_file} ${output_file} [g_name_weight]
   ```

   **参数说明：**

   * \${input_file} 输入文件（预聚类步骤的输出）在 HDFS 中路径
   * \${output_file} 输出文件在 HDFS 中路径
   * g_name_weight 在 g_name 中出现的词的额外权重，用于调整 g_name 和 g_model 的重要性。double型，默认为 1.0，即认为 g_model 和 g_name 一样重要。

   *注意事项：*

   实际输出文件保存在 \${output_file}/result 中

4. 构建倒排索引：

   读入单词权重的计算结果， 根据倒排索引的原理，将权重的结果插入到倒排索引中，生成带权重信息的倒排索引。对应 Driver 类用法：

   ```bash
   hadoop jar clustering.inverted_index.Driver ${input_file} ${output_file} [decimal_number] [pruning_threshold]
   ```

   **参数说明：**

   * \${input_file} 输入文件（权重计算步骤的输出）在 HDFS 中路径
   * \${output_file} 输出文件在 HDFS 中路径
   * decimal_number 输出时保留的小数位数。关系到距离计算步骤的效率和精度，默认为4位
   * pruning_threshold 剪枝阈值。double 类型，默认为不剪枝。当传入该参数时，小于该参数的单词权值会被忽略。不建议使用。

5. 计算相似度矩阵：

   读入倒排索引，通过倒排索引中包含的权重信息，分布式计算向量（商品）空间的距离矩阵。由于计算量较大，耗时长，该步骤被划分为两步完成。第一步 Driver 类用法：

   ```bash
   hadoop jar clustering.similarity.PreDriver ${input_file} ${output_file} [compression_flag] [reducer_number] [deci_number]
   ```

   **参数说明：**

   - \${input_file} 输入文件（生成倒排索引步骤的输出）在 HDFS 中路径
   - \${output_file} 输出文件在 HDFS 中路径
   - compression_flag 1 为使用压缩格式进行输出，0 不使用。由于计算结果占用大量存储空间，使用压缩格式能显著缩短 IO 时间。默认为 1
   - reducer_number reducer 作业的数目。参考集群所能开启的 mapper / reducer 容器数。在为 Resource Manager / App Manager 预留足够容器后，建议剩余容器全部用于开启 reducer 作业，以增加计算的并行度。默认值为29
   - deci_number 输出时保留的小数位数。参考生成倒排索引时的设置

6. 分布式层次聚类运算

   ​读入计算出的相似度矩阵，

7. ​

