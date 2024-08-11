# MIT6.824
## 1.lab1-mapreduce
主要课程目标：实现一个mapreduce框架，可以接收<span style="color:RED;">用户写的自定义  map函数和reduce函数(mrapps)</span>给出输出。  

代码结构：main里面的代码不能改动，是主程序入口。主要修改mr目录下的master、worker、rpc  

实现过程：每个worker将在一个循环中向协调器请求一项任务，从一个或多个文件读取任务的输入，执行该任务，将任务的输出写入一个或多个文件，然后再次向coordinator请求一项新任务。coordinator如果10S内没收到worker发来的回复，则认为其任务失败。

