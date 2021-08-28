package org.n0nb0at.spark.distcp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object DistCp extends Logging {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("Spark DistCP").master("local").getOrCreate()

    // 参数解析
    val config = OptionsParsing.parse(args, sparkSession.sparkContext.hadoopConfiguration)

    logInfo("parsed config: " + config.toString)

    val (source, target) = config.sourceAndDestPaths

    val fileList = new ArrayBuffer[(Path, Path)]

    // 检查目录
    checkDir(sparkSession, source.head, target, fileList, config.options)

    // 执行复制
    copy(sparkSession, fileList, config.options)
  }

  /**
   * 检查目录结构并获取文件列表
   *
   * @param sparkSession spark会话
   * @param sourcePath   源路径
   * @param targetPath   目标路径
   * @param fileList     <source: Path, target: Path> 文件路径列表
   * @param options      命令行参数
   */
  def checkDir(sparkSession: SparkSession, sourcePath: Path, targetPath: Path,
               fileList: ArrayBuffer[(Path, Path)], options: SparkDistCPOptions): Unit = {

    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    fs.listStatus(sourcePath)
      .foreach(currPath => {
        // 当前是目录的话，进行目标路径创建
        if (currPath.isDirectory) {
          val subPath = currPath.getPath.toString.split(sourcePath.toString)(1)
          val mkdirPath = targetPath + subPath
          val nextTargetPath = new Path(mkdirPath)
          logInfo("mkdir " + mkdirPath)
          try {
            fs.mkdirs(nextTargetPath)
          } catch {
            // 创建文件目录可能抛出异常，根据命令行参数决定抛出或忽略
            case ex: Exception => if (!options.ignoreFailures) throw ex else logWarning(ex.getMessage)
          }
          // 递归检查下一级
          checkDir(sparkSession, currPath.getPath, nextTargetPath, fileList, options)
        } else { // 当前是文件，则将源、目标文件成对保存到列表中
          fileList.append((currPath.getPath, targetPath))
        }
      })
  }

  /**
   * 执行复制
   *
   * @param sparkSession spark会话
   * @param fileList     <source: Path, target: Path> 文件路径列表
   * @param options      命令行参数
   */
  def copy(sparkSession: SparkSession, fileList: ArrayBuffer[(Path, Path)], options: SparkDistCPOptions): Unit = {
    val sc = sparkSession.sparkContext
    // 最大并行任务数，命令行没设置的情况下默认 1000
    val maxConcurrenceTask = Some(options.maxConcurrenceTask).getOrElse(1000)
    val rdd = sc.makeRDD(fileList, maxConcurrenceTask)

    rdd.mapPartitions(ite => {
      val hadoopConf = new Configuration()
      ite.foreach(tup => {
        try {
          // 使用 Hadoop 复制文件工具类
          FileUtil.copy(tup._1.getFileSystem(hadoopConf), tup._1,
            tup._2.getFileSystem(hadoopConf), tup._2,
            false, hadoopConf)
          logInfo("copy from " + tup._1.toString + " to " + tup._2.toString)
        } catch {
          // 根据命令行参数决定抛出或忽略
          case ex: Exception => if (!options.ignoreFailures) throw ex else logWarning(ex.getMessage)
        }
      })
      // o(╥﹏╥)o
      // mapPartitions 的用法不熟练，一开始没写这个 ite 返回，IDE 运行不断报错……
      // 因为太低级了，网上都搜索不到，全是在说需要同步类型、强制转换之类的
      ite
    }).collect()
  }
}