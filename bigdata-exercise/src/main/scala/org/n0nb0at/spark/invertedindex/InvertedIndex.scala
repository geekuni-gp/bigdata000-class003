package org.n0nb0at.spark.invertedindex

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.{SparkConf, SparkContext}

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    // 环境配置，使用本地
    val sparkConf = new SparkConf().setAppName("InvertedIndex").setMaster("local")
    val sc = new SparkContext(sparkConf)

    if (args.length != 2) {
      println("参数错误，入参为：[输入文件路径] [输出文件路径] ")
    }

    val inputPath: String = args.apply(0)
    val outputPath: String = args.apply(1)

    // 获取输入文件
    val fc = classOf[TextInputFormat]
    val kc = classOf[LongWritable]
    val vc = classOf[Text]
    val text = sc.newAPIHadoopFile(inputPath, fc, kc, vc, sc.hadoopConfiguration)

    // 抽取文件、行记录对应关系 <文件名, 文件单词集合>
    val linesWithFileNames = text.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit((inputSplit, iterator) => {
        val file = inputSplit.asInstanceOf[FileSplit]
        iterator.map(tup => (file.getPath.toString.split("/").last, tup._2))
      })

    // 按单词组织成临时索引 <单词, 文件名>
    val tempIndex = linesWithFileNames.flatMap {
      case (fileName, text) => text.toString.split("\r\n")
        .flatMap(line => line.split(" "))
        .map { word => (word, fileName) }
    }

    // 分组聚合 <单词, 文件名集合>
    val invertedIndex = tempIndex.groupByKey()

    // 词频统计 <单词, <文件名集合, 词频>>
    val group = invertedIndex.map {
      case (word, tup) =>
        val fileCountMap = scala.collection.mutable.HashMap[String, Int]()
        for (fileName <- tup) {
          val count = fileCountMap.getOrElseUpdate(fileName, 0) + 1
          fileCountMap.put(fileName, count)
        }
        (word, fileCountMap)
    }.sortByKey().map(word => s"${word._1}:${word._2}")

    // 命令行输出
    group.repartition(1).foreach(println)
    // 文件输出
    group.repartition(1).saveAsTextFile(outputPath)
  }
}
