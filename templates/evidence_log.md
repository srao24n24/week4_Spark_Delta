# Week 4 evidence log
| Day | Code submitted | Output path | Key counts/totals | explain/history evidence | What I learned |
|---|---|---|---|---|---|
| 1 | starter/day1_dataframe_foundations.py | work/day1/orders_selected (Parquet) | 20 orders read, 1 partition, 8 selected (from lab 1.3 filter), and 8 read back from Parquet | Console output evidence in work/day1. Formatted physical plan captured before and after the action. The plan shows Scan csv to Filter to Project, with filters pushed to the CSV scan. No plan change occurred after the action. The transformations are select(), filter(), and withColumn(). count() and show() are actions that trigger execution. | SparkSession is the main entry point for using Spark. It lets you create DataFrames and run Spark operations. The driver is the main program that controls the Spark application. It creates the execution plan and assigns work. An executor is a worker process that receives tasks from the driver and processes data. A partition is a smaller chunk of data. Spark divides large datasets into partitions so they can be processed in parallel. Lazy evaluation means Spark does not immediately execute transformations like filter(), select(), or withColumn(). Instead, it builds a plan and waits until an action, such as show(), count(), or write() to trigger execution. |
| 2 | | | | | |
| 3 | | | | | |
| 4 | | | | | |
| 5 | | | | | |
| 6 | | | | | |