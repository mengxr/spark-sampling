package org.apache.spark.sampling

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.SparkContext

import org.apache.spark.sampling.RDDSamplingFunctions._
import org.apache.log4j.{Level, Logger}

class RDDSamplingFunctionsSuite extends FunSuite with BeforeAndAfterAll {

  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
    Logger.getRootLogger.setLevel(Level.WARN)
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  test("ScaSRS") {
    val n = 10000L
    val data = sc.parallelize(0L until n, 5)
    for (p <- Seq(0.01, 0.05, 0.1, 0.5); seed <- (0L until 5L)) {
      val s = math.ceil(p * n).toLong
      val sampled = data.sampleWithoutReplacement(p, n, seed)
      assert(sampled.count() === s)
    }
  }

  test("ScaSRSWR") {
    val n = 10000L
    val data = sc.parallelize(0L until n, 5)
    for (p <- Seq(0.01, 0.05, 0.1, 0.5, 1.0); seed <- (0L until 5L)) {
      val s = math.ceil(p * n).toLong
      val sampled = data.sampleWithReplacement(s, n, seed)
      assert(sampled.count() === s)
    }
  }

  test("ScaSRSWR (new)") {
    val n = 10000L
    val data = sc.parallelize(0L until n, 5)
    for (p <- Seq(0.01, 0.05, 0.1, 0.5, 1.0); seed <- (0L until 5L)) {
      val s = math.ceil(p * n).toLong
      val sampled = data.sampleWithReplacementNew(s, n, seed)
      assert(sampled.count() === s)
    }
  }
}
