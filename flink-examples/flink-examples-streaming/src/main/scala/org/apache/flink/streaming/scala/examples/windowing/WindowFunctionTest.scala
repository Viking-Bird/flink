package org.apache.flink.streaming.scala.examples.windowing

import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration
import java.util.Date

/**
 * 窗口函数测试，测试数据：
 *
 * spark,20,1593050400000
 * spark,30,1593050401000
 * flink,20,1593050404000
 * spark,40,1593050405000
 *
 * spark,100,1593050406000
 * flink,200,1593050407000
 * flink,300,1593050410000
 *
 * flink,10,1593050415000
 *
 * @Author: wangpeng
 * @Date: 2022/4/24 21:42
 */
class WindowFunctionTest {

  val HOSTNAME = "localhost"
  val PORT = 7777
  var DATA = List("spark,20,1593050400000", "spark,30,1593050401000", "flink,20,1593050404000", "spark,40,1593050405000", "spark,100,1593050406000", "flink,200,1593050407000", "flink,300,1593050410000", "flink,10,1593050415000")

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  /**
   * ReduceFunction 测试
   */
  @Test
  def reduceFuncTest: Unit = {
    val text = env.fromCollection(DATA)
    val reduceStream = text
      .map(r => r.split(","))
      .filter(_.nonEmpty)
      .map(p => Tuple3[String, Int, Long](p(0), p(1).toInt, p(2).toLong))
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.minutes(15)))
      .reduce(new CustomReduceFunction, new CustomReduceProcessFunction)
    //      .reduce(new CustomReduceFunction)

    // print也是一种特殊类型的sink
    reduceStream.print("ReduceFunction example")
    env.execute("ReduceFunction job")
  }

  /**
   * AggregateFunction 测试
   */
  @Test
  def aggregateFuncTest: Unit = {
    val text = env.fromCollection(DATA)
    text.map(_.toLowerCase.split(",")).filter(_.nonEmpty)
      .map(p => Tuple3[String, Int, Long](p(0), p(1).toInt, p(2).toLong))
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.minutes(10)))
      //      .aggregate(new CustomAggregateFunc
      .aggregate(new CustomAggregateFunc, new CustomAggProcessFunction)
      .print()

    env.execute()
  }

  /**
   * ProcessWindowFunction 测试
   */
  @Test
  def processFuncTest: Unit = {
    val text = env.fromCollection(DATA)
    text.map(_.toLowerCase.split(",")).filter(_.nonEmpty)
      .map(p => Tuple3[String, Int, Long](p(0), p(1).toInt, p(2).toLong))
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.minutes(10)))
      .process(new CustomProcessFunction)
      .print()

    env.execute("CustomProcessFunction")
  }

  /**
   * Evictor 测试
   */
  @Test
  def countEvictorOpsTest: Unit = {
    val text = env.fromCollection(DATA)
    text.map(_.toLowerCase.split(",")).filter(_.nonEmpty)
      .map(p => Tuple3[String, Int, Long](p(0), p(1).toInt, p(2).toLong))
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.minutes(10)))
      .evictor(CountEvictor.of[TimeWindow](2l)) // Evictor保留个数为2
      .process(new CustomProcessFunction)
      .print()

    env.execute("CustomProcessFunction")
  }

  /**
   * allowedLateness延迟功能测试，把延迟数据输出到侧输出流
   */
  @Test
  def allowedLatenessTest: Unit = {
    DATA = List("spark,20,1593050400000",
      "spark,30,1593050401000",
      "flink,10,1593050402000",
      "spark,40,1593050405000",
      "spark,100,1593050406000",
      "spark,10,1593050390000",
      "flink,200,1593050407000",
      "spark,10,1593050310000")
    val text = env.socketTextStream(HOSTNAME, PORT)

    // 延迟数据侧输出流标签
    val lateOutputTag: OutputTag[(String, Int, Long)] = new OutputTag[(String, Int, Long)]("late-data")

    val res = text.map(_.toLowerCase.split(",")).filter(_.nonEmpty)
      .map(p => Tuple3[String, Int, Long](p(0), p(1).toInt, p(2).toLong))
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[(String, Int, Long)](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Int, Long)] {
          override def extractTimestamp(element: (String, Int, Long), recordTimestamp: Long): Long = element._3
        }))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      //      .aggregate(new CustomAggregateFunc
      .allowedLateness(Time.seconds(3))
      .sideOutputLateData(lateOutputTag)
      .sum(1)

    res.print("主流：")
    res.getSideOutput(lateOutputTag).map(_.toString() + " is late").print()

    env.execute()
  }
}

/**
 * 自定义watermark时间字段提取器
 */
class CustomWatermarkAssigner extends BoundedOutOfOrdernessTimestampExtractor[(String, Int, Long)](Time.seconds(2)) {
  override def extractTimestamp(element: (String, Int, Long)): Long = {
    element._3
  }
}

/**
 * 自定义ReduceFunction：将两个相同类型的元素合并成一个，输入元素的类型和输出结果的类型都要求一致
 */
class CustomReduceFunction extends ReduceFunction[(String, Int, Long)] {
  override def reduce(value1: (String, Int, Long), value2: (String, Int, Long)): (String, Int, Long) = {
    Tuple3(value1._1, value1._2 + value2._2, value2._3)
  }
}

/**
 * 自定义ProcessWindowFunction
 */
class CustomReduceProcessFunction extends ProcessWindowFunction[(String, Int, Long), String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Int, Long)], out: Collector[String]): Unit = {
    val aggValue = elements.iterator.next()._2
    val windowStart = DateFormatUtils.format(new Date(context.window.getStart), "yyyy-MM-dd HH:mm:ss")
    val windowEnd = DateFormatUtils.format(new Date(context.window.getEnd), "yyyy-MM-dd HH:mm:ss")

    val record = "Key: " + key + " 窗口开始时间: " + windowStart + " 窗口结束时间: " + windowEnd + " 累计值: " + aggValue
    out.collect(record)
  }
}

/**
 * 自定义AggregateFunction，求平均值
 */
class CustomAggregateFunc extends AggregateFunction[(String, Int, Long), (Int, Int), Double] {
  // 构建初始状态值
  override def createAccumulator(): (Int, Int) = {
    (0, 0)
  }

  // 输入值与中间状态值相加
  override def add(value: (String, Int, Long), accumulator: (Int, Int)): (Int, Int) = {
    (value._2 + accumulator._1, accumulator._2 + 1)
  }

  // 获取最终的结果
  override def getResult(accumulator: (Int, Int)): Double = {
    accumulator._1 / accumulator._2
  }

  // 两个中间状态的融合
  override def merge(a: (Int, Int), b: (Int, Int)): (Int, Int) = {
    (a._1 + b._1, a._2 + b._2)
  }
}

/**
 * 自定义ProcessWindowFunction
 */
class CustomProcessFunction extends ProcessWindowFunction[(String, Int, Long), (String, Int), String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Int, Long)], out: Collector[(String, Int)]): Unit = {
    var count: Int = 0
    for (i <- elements) {
      count = count + i._2
    }
    out.collect((key, count))
  }
}

/**
 * 自定义ProcessWindowFunction
 */
class CustomAggProcessFunction extends ProcessWindowFunction[Double, String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Double], out: Collector[String]): Unit = {
    val aggValue = elements.iterator.next()
    val windowStart = DateFormatUtils.format(new Date(context.window.getStart), "yyyy-MM-dd HH:mm:ss")
    val windowEnd = DateFormatUtils.format(new Date(context.window.getEnd), "yyyy-MM-dd HH:mm:ss")

    val record = "Key: " + key + " 窗口开始时间: " + windowStart + " 窗口结束时间: " + windowEnd + " 平均值: " + aggValue
    out.collect(record)
  }
}
