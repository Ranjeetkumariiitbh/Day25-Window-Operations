package com.day25

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream

object Day25WindowOperations {

  // Production scenario:
  //   batch interval = 2 seconds
  //   window size    = 10 minutes
  //   sliding interval = 2 minutes
  //
  // For a quick local demo, pass: demo
  private val ProductionWindowSeconds = 600
  private val ProductionSlideSeconds  = 120

  def main(args: Array[String]): Unit = {
    val demo = args.headOption.contains("demo")

    val batchSeconds = if (demo) 2 else 2
    val windowSeconds = if (demo) 10 else ProductionWindowSeconds
    val slideSeconds  = if (demo) 4 else ProductionSlideSeconds

    val conf = new SparkConf()
      .setAppName("Day25-Window-Operations")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(batchSeconds))
    ssc.checkpoint("/tmp/day25-checkpoint")
    ssc.sparkContext.setLogLevel("WARN")

    // Receiver-based stream: each line is a transaction:
    // timestamp,customerId,category,amount
    //
    // Example:
    // 2026-10-02T10:00:00,C01,electronics,1200
    val lines = ssc.socketTextStream("localhost", 9999)

    val transactions = lines.flatMap(parseTransaction)

    // ------------------------------------------------------------
    // 1) countByWindow
    // ------------------------------------------------------------
    // Counts transactions arriving in the last `windowSeconds`.
    // The result is refreshed every `slideSeconds`.
    val transactionCount: DStream[Long] =
      transactions.countByWindow(Seconds(windowSeconds), Seconds(slideSeconds))

    transactionCount.foreachRDD { (rdd, time: Time) =>
      val count = rdd.collect().headOption.getOrElse(0L)
      val status =
        if (count >= 5) "ALERT: sudden increase in transactions"
        else "NORMAL"

      println(
        s"[countByWindow] time=$time | " +
        s"last_${windowSeconds}s_transactions=$count | $status"
      )
    }

    // ------------------------------------------------------------
    // 2) reduceByKeyAndWindow
    // ------------------------------------------------------------
    // Rolling sales total per category.
    val categorySales = transactions
      .map { case (_, _, category, amount) => (category, amount) }
      .reduceByKeyAndWindow(
        (a: Double, b: Double) => a + b,
        Seconds(windowSeconds),
        Seconds(slideSeconds)
      )

    categorySales.foreachRDD { (rdd, time: Time) =>
      println(s"[reduceByKeyAndWindow] time=$time | rolling sales:")
      rdd.collect().sortBy(_._1).foreach {
        case (category, total) =>
          println(f"  $category%-15s -> ₹$total%.2f")
      }
    }

    // ------------------------------------------------------------
    // 3) Per-customer rolling transaction totals
    // ------------------------------------------------------------
    val customerCount = transactions
      .map { case (_, customer, _, _) => (customer, 1L) }
      .reduceByKeyAndWindow(
        (a: Long, b: Long) => a + b,
        Seconds(windowSeconds),
        Seconds(slideSeconds)
      )

    customerCount.foreachRDD { (rdd, time: Time) =>
      println(s"[customer rolling count] time=$time")
      rdd.collect().sortBy(-_._2).foreach {
        case (customer, count) =>
          println(s"  $customer -> $count transactions")
      }
    }

    println(
      s"""
         |Day 25 — Window Operations
         |---------------------------------
         |Batch interval   : ${batchSeconds}s
         |Window size      : ${windowSeconds}s
         |Sliding interval : ${slideSeconds}s
         |Input             : localhost:9999
         |
         |Start a socket source with:
         |  nc -lk 9999
         |
         |Then run this application.
         |""".stripMargin
    )

    ssc.start()
    ssc.awaitTermination()
  }

  private def parseTransaction(
      line: String
  ): Option[(String, String, String, Double)] = {
    val parts = line.split(",", -1).map(_.trim)

    if (parts.length == 4) {
      try {
        Some((parts(0), parts(1), parts(2), parts(3).toDouble))
      } catch {
        case _: NumberFormatException => None
      }
    } else {
      None
    }
  }
}
