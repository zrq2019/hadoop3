## <font color='midblue'>作业7 - 编写MapReduce程序对鸢尾花数据集进行分类预测</font>

### 1 作业简述

​        Iris数据集是常用的分类实验数据集，由Fisher, 1936收集整理。Iris也称鸢尾花卉数据集，是一类多重变量分析的数据集。数据集包含150个数据，分为3类，每类50个数据，每个数据包含4个属性。可通过花萼长度，花萼宽度，花瓣长度，花瓣宽度4个属性预测鸢尾花卉属于（Setosa，Versicolour，Virginica）三个种类中的哪一类。在MapReduce上任选一种分类算法（KNN，朴素贝叶斯或决策树）对该数据集进行分类预测，采用留出法对建模结果评估，70%数据作为训练集，30%数据作为测试集，评估标准采用精度accuracy。可以尝试对结果进行可视化的展示（可选）。

### 2 设计思路

#### 2.1 数据预处理

​       使用 python ，采用留出法（hold-out）将数据集划分为两个互斥的部分，其中70%作为训练集，30%用作测试集。使用 sklearn 中的 train_test_split () 函数拆分训练集，测试集并输出到相应的文件中。

```python
iris = datasets.load_iris()
X = iris.data
y = iris.target

# 拆分训练集、测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)

pd.DataFrame(X_train).to_csv('./X_train.csv', header=False, index=False)
pd.DataFrame(y_train).to_csv('./y_train.csv', header=False, index=False)
pd.DataFrame(X_test).to_csv('./X_test.csv', header=False, index=False)
pd.DataFrame(y_test).to_csv('./y_test.csv', header=False, index=False)
```

#### 2.2 文件结构

​       **选择 KNN 分类算法：**

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-05 173230.jpg)

- Distance.java：计算两点之间的距离
- Instance.java：将数据集的数据分别读入attributeValue和lable中
- KNearestNeighbour：主程序

#### 2.3 核心函数实现

​       Map：

```java
public void map(LongWritable textIndex, Text textLine, Context context)
				throws IOException, InterruptedException 
{
			//distance stores all the current nearst distance value
			//. trainLable store the corresponding lable
			ArrayList<Double> distance = new ArrayList<Double>(k);
			ArrayList<DoubleWritable> trainLable = new ArrayList<DoubleWritable>(k);
			for(int i = 0;i < k;i++)
			{
				distance.add(Double.MAX_VALUE);
				trainLable.add(new DoubleWritable(-1.0));
			}
			ListWritable<DoubleWritable> lables = new ListWritable<DoubleWritable>(DoubleWritable.class);		
			Instance testInstance = new Instance(textLine.toString());
			for(int i = 0;i < trainSet.size();i++)
			{
				try {
					double dis = Distance.EuclideanDistance(trainSet.get(i).getAtrributeValue(), testInstance.getAtrributeValue());
					int index = indexOfMax(distance);
					if(dis < distance.get(index))
					{
						distance.remove(index);
					    trainLable.remove(index);
					    distance.add(dis);
					    trainLable.add(new DoubleWritable(trainSet.get(i).getLable()));
					}
				} 
                catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
			}			
			lables.setList(trainLable);	
		    context.write(textIndex, lables);
}
```

​       Reduce：

```java
public void reduce(LongWritable index, Iterable<ListWritable<DoubleWritable>> kLables, Context context)
				throws IOException, InterruptedException
{
			/**
			 * each index can actually have one list because of the
			 * assumption that the particular line index is unique
			 * to one instance.
			 */
			DoubleWritable predictedLable = new DoubleWritable();
			for(ListWritable<DoubleWritable> val: kLables)
            {
				try {
					predictedLable = valueOfMostFrequent(val);
					break;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			context.write(NullWritable.get(), predictedLable);
}
```

​        main：

```java
public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
{
		Job kNNJob = new Job();
		kNNJob.setJobName("kNNJob");
		kNNJob.setJarByClass(KNearestNeighbour.class);
		DistributedCache.addCacheFile(URI.create(args[2]), kNNJob.getConfiguration());
		kNNJob.getConfiguration().setInt("k", Integer.parseInt(args[3]));
		
		kNNJob.setMapperClass(KNNMap.class);
		kNNJob.setMapOutputKeyClass(LongWritable.class);
		kNNJob.setMapOutputValueClass(ListWritable.class);

		kNNJob.setReducerClass(KNNReduce.class);
		kNNJob.setOutputKeyClass(NullWritable.class);
		kNNJob.setOutputValueClass(DoubleWritable.class);

		kNNJob.setInputFormatClass(TextInputFormat.class);
		kNNJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(kNNJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(kNNJob, new Path(args[1]));
		
		kNNJob.waitForCompletion(true);
		System.out.println("finished!");
}
```



### 3 实验结果

#### 3.1 使用python计算精度accuracy对预测结果进行评估

```python
data = pd.read_csv('y_test.csv')    # 正确结果
data1 = pd.read_csv('y_hh.csv')     # 预测结果
# 评分
print(accuracy_score(data, data1, normalize=True, sample_weight=None))
```

​        运行结果如下：

![](C:\Users\THINK\Desktop\屏幕截图 2021-11-05 180502.jpg)

​        可以看出，预测的精度很高，knn算法在鸢尾花数据集上表现良好。

#### 3.2 提交作业运行成功的WEB页面截图

![](E:\金融大数据\作业\屏幕截图 2021-10-28 140501.jpg)



### 4 收获

​        通过这次作业，我掌握了用MapReduce实现分类算法的基本方法，对于 java 各个类之间的调用以及限定词越来越理解，虽然 java 在机器学习方面不如 python 方便，但是正因为这种不便使得我们得以更深入地了解算法的实现细节，而不是轻松地调包，相信今后我对 MapReduce 以及 Hadoop 相关知识的掌握会越来越好！
