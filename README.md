>电商分析平台

该项目是我根据尚硅谷大数据电商分析平台视频做的笔记,总共分成了大概十个需求,每个需求我都用一篇文章来解析


## 项目文章目录:
[项目搭建及,commons模块解析,离线实时数据准备](https://blog.csdn.net/zisuu/article/details/106361630)

[项目需求解析](https://blog.csdn.net/zisuu/article/details/106302167)



[需求一:各个范围Session步长、访问时长占比统计](https://blog.csdn.net/zisuu/article/details/106329092)


[需求二:按照比列随机抽取session](https://blog.csdn.net/zisuu/article/details/106333719)

[需求三:热门top10商品](https://blog.csdn.net/zisuu/article/details/106335694)

[需求四:Top10热门品类的Top10活跃Session统计](https://blog.csdn.net/zisuu/article/details/106338047)

[需求五:计算给定的页面访问流的页面单跳转化率](https://blog.csdn.net/zisuu/article/details/106341485)

[需求六:实时统计之黑名单机制](https://blog.csdn.net/zisuu/article/details/106354769)


[需求七,九前置知识](https://blog.csdn.net/zisuu/article/details/106358260)

[需求七:实时统计之各省各城市广告点击量实时统计](https://blog.csdn.net/zisuu/article/details/106356262)

[需求八:实时统计之各省份广告top3排名](https://blog.csdn.net/zisuu/article/details/106357644)


[需求九:实时统计之最近一小时广告点击量实时统计](https://blog.csdn.net/zisuu/article/details/106359362)

[需求十:总结](https://blog.csdn.net/zisuu/article/details/106359657)


## 项目整体概述
**课程简介**
>本课程是一套完整的企业级电商大数据分析系统，在当下最为热门的Spark生态体系基础上构建企业级数据分析平台，本系统包括离线分析系统与实时分析系统，技术栈涵盖Spark Core，Spark SQL，Spark Streaming与Spark性能调优，并在课程中穿插Spark内核原理与面试要点，能够让学员在实战中全面掌握Spark生态体系的核心技术框架。
本课程内容丰富，所有需求均来自于企业内部，对于每一个具体需求，讲师全部采用文字与图片相结合的讲解方式，从零实现每一个需求代码并对代码进行逐行解析，让学员知其然并知其所以然，通过本课程，能够让你对Spark技术框架的理解达到新的高度。

**如何学习?**


- 到github下载源码(顺便给个start噢!)
地址:[spark-shopAnalyze](https://github.com/zisuu870/spark-shopAnalyze)

- 根据目录中第一篇文章,理解commons模块和mock模块的作用,并跟着文章创建一个maven工程!!!这个是很重要的,
- 根据目录中第二篇文章,理解需求的大概内容,
- 跟着目录顺序,理解每个需求的大致内容,然后一定要自己手打一遍
- 每做完一个需求,总结该需求所学
- 遇到不会的算子自查百度


 
**所用技术框架**
- spark(spark-sql,spark-streaming-spark-sql)
- hive
- kafka
- mysql
- hadoop-hdfs

**所需环境**

- hadoop
>本人是利用virtualBox搭建了hadoop的完全分布式环境如果你还没有hadoop环境,可以参考下面两篇文章:

[【超详细】最新Windows下安装Virtual Box后安装CentOS7并设置双网卡实现宿主机与虚拟机互相访问](https://blog.csdn.net/adamlinsfz/article/details/84108536)
 [【超详细】最新VirtualBox+CentOS7+Hadoop2.8.5手把手搭建完全分布式Hadoop集群（从小白逐步进阶）](https://blog.csdn.net/adamlinsfz/article/details/84333389)

- IDEA scala,spark开发环境

[如何用Idea运行我们的Spark项目](https://www.cnblogs.com/tjp40922/p/12177913.html)

- sparkStreaming与kafka的整合

[Spark Streaming整合Kafka](https://www.jianshu.com/p/ec3bf53dcf3f)





**主要功能**
主要分为离线统计和实时统计两部分,共分十个需求,每个需求一篇文章进行详解,保证能看的懂
![在这里插入图片描述](https://img-blog.csdnimg.cn/20200526170258516.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3ppc3V1,size_16,color_FFFFFF,t_70)

**你能学到什么?**

- 整合hadoop-hdfs,kafka,spark,spark-sql,spark-streaming,hive等大数据常用框架,对所学知识起到梳理作用
- 对spark的各个算子,以及spark-sql,spark-streaming深入理解.这个教程主要的核心框架就是spark
- 知道常见的大数据计算模式,懂得如何对计算需求进行分析,逆推,并且做到活学活用


## 项目模块分析

**项目目录:**


![在这里插入图片描述](https://img-blog.csdnimg.cn/20200526171831646.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3ppc3V1,size_16,color_FFFFFF,t_70)
**commons模块**

>commons主要用于一些配置读取,对象连接池获取,代码规范等

![在这里插入图片描述](https://img-blog.csdnimg.cn/20200526171935541.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3ppc3V1,size_16,color_FFFFFF,t_70)

**mock模块**
- mock模块主要用于模拟数据的获取,
- MockDataGenerate用于产生离线的数据,你可以选择保存到hadoop中,亦或者保存到hive中.如果你还没学过hive,那就保存到hadoop
- MockRealTimeData用于产生实时数据,并通过kafka将数据发送到sparkStreaming,以便统计实时的数据




![在这里插入图片描述](https://img-blog.csdnimg.cn/20200526172158317.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3ppc3V1,size_16,color_FFFFFF,t_70)
**sesion模块**

- sesion模块是离线数据统计的模块
- 其中sessionStat是主函数所在处
- server目录下的各个server是各个需求的代码处,会通过主函数sessionStat进行引用
- sessionAccumulator是自定义的累加器
- SortKey是自定义排序器


![在这里插入图片描述](https://img-blog.csdnimg.cn/20200526172431181.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3ppc3V1,size_16,color_FFFFFF,t_70)

**adverStat模块**

- advertStat是主函数所在
- 因为实时部分的需求是上下相互关联的,所以都在一个主函数中进行调用
- jdbcHelper,可视为java中的dao层

![在这里插入图片描述](https://img-blog.csdnimg.cn/20200526172729749.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3ppc3V1,size_16,color_FFFFFF,t_70)

