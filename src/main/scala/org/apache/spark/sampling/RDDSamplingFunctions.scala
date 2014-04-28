package org.apache.spark.sampling

import java.util.Random

import scala.collection.mutable.{ArrayBuffer, HashSet => MutableHashSet}
import scala.reflect.ClassTag

import org.apache.commons.math3.random.RandomDataImpl
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

class RDDSamplingFunctions[T: ClassTag](self: RDD[T]) extends Logging with Serializable {

  def sampleWithoutReplacement(p: Double, n1: Long = self.count(), seed: Long = System.nanoTime())
  : RDD[T] = {
    val sc = self.context
    val accNumItems = sc.accumulator[Long](0L)
    val accNumAccepted = sc.accumulator[Long](0L)
    val accWaitlisted = sc.accumulableCollection[ArrayBuffer[Double], Double](new ArrayBuffer[Double]())
    // Compute thresholds to accept (q1) and reject (q2) items.
    val t1 = 20.0 / (3.0 * n1)
    val q1 = p + t1 - math.sqrt(t1 * t1 + 3.0 * t1 * p)
    val t2 = 10.0 / n1
    val q2 = p + t2 + math.sqrt(t2 * t2 + 2.0 * t2 * p)
    self.foreachWith((idx: Int) => new Random(seed + idx)){(item: T, random: Random) =>
      accNumItems += 1L
      val x = random.nextDouble()
      if (x < q1) {
        accNumAccepted += 1L
      } else if (x < q2) {
        accWaitlisted += x
      }
    }
    val n = accNumItems.value
    logInfo("The population size is " + n + ".")
    if (n < n1) {
      logError("The input lower bound " + n1 + " is greater than total number of items " + n + ".")
    }
    val s = math.ceil(p * n).toLong
    logInfo("The desired sample size is " + s + ".")
    val numAccepted = accNumAccepted.value
    logInfo("Pre-accepted " + numAccepted + " items.")
    var threshold = p
    val waitlisted = accWaitlisted.value
    logInfo("Waitlisted " + waitlisted.size + " items.")
    val numWaitlistAccepted = (s - numAccepted).toInt
    if (numWaitlistAccepted >= waitlisted.size) {
      logWarning("Waitlist is too short!")
      threshold = q2
    } else if (numWaitlistAccepted > 0) {
      threshold = waitlisted.sorted.apply((s - numAccepted).toInt)
    } else {
      logWarning("Pre-accepted too many!")
      threshold = q1
    }
    logInfo("Set final threshold to " + threshold + ".")
    self.mapPartitionsWithIndex((idx: Int, iter: Iterator[T]) => {
      val random = new Random(seed + idx)
      iter.filter(t => random.nextDouble() < threshold)
    }, preservesPartitioning = true)
  }

  def sampleWithReplacement(s: Long, n: Long = self.count(), seed: Long = System.nanoTime()): RDD[T] = {
    val failureRate = 1e-4
    val threshold = 1.0 - math.exp(math.log(failureRate / s) / n)
    val voted = self.mapPartitionsWithIndex((idx: Int, iter: Iterator[T]) => {
        val random = new RandomDataImpl()
        random.reSeed(seed + idx)
        iter.map { t =>
          (random.nextBinomial(s.toInt, threshold), t)
        }.filter(_._1 > 0).flatMap { case (i, t) =>
          sampleWithoutReplacement(s, i, random).map { pos =>
            (pos, (random.nextUniform(0.0, 1.0), t))
          }
        }
      }, preservesPartitioning = true)
    voted.reduceByKey { (v1: (Double, T), v2: (Double, T)) =>
      if (v1._1 < v2._1) v1 else v2
    }.map(_._2._2)
  }

  def sampleWithReplacementNew(s: Long, n: Long = self.count(), seed: Long = System.nanoTime()): RDD[T] = {
    val lmbd1 = PoissonBounds.getLambda1(s)
    println("lmbd1 = " + lmbd1)
    val minCount = PoissonBounds.getMinCount(lmbd1)
    println("minCount = " + minCount)
    val lmbd2 = if (lmbd1 == 0) PoissonBounds.getLambda2(s) else PoissonBounds.getLambda2(s-minCount)
    println("lmbd2 = " + lmbd2)
    val p1 = lmbd1/n
    val p2 = lmbd2/n
    val sc = self.context
    val accNumAccepted = sc.accumulator[Long](0L)
    val accWaitlisted = sc.accumulableCollection[ArrayBuffer[Double], Double](new ArrayBuffer[Double]())
    self.foreachWith((idx: Int) => {
      val random = new RandomDataImpl()
      random.reSeed(seed + idx)
      random
    }){ (item: T, random: RandomDataImpl) =>
      val x1 = if (p1 == 0) 0L else random.nextPoisson(p1)
      if (x1 > 0) {
        accNumAccepted += x1
      }
      val x2 = random.nextPoisson(p2).toInt
      if (x2 > 0) {
        accWaitlisted ++= ArrayBuffer.fill(x2)(random.nextUniform(0.0, 1.0))
      }
    }
    val numAccepted = accNumAccepted.value
    val waitlisted = accWaitlisted.value.sorted
    println("numAccepted = " + numAccepted)
    println("numWaitlisted = " + waitlisted.size)
    val threshold = waitlisted((s-numAccepted).toInt)
    self.mapPartitionsWithIndex((idx: Int, iter: Iterator[T]) => {
      val random = new RandomDataImpl()
      random.reSeed(seed + idx)
      iter.flatMap { t =>
        val x1 = if (p1 == 0) 0L else random.nextPoisson(p1)
        val x2 = random.nextPoisson(p2).toInt
        val x = x1 + (0 until x2).filter(i => random.nextUniform(0.0, 1.0) < threshold).size
        if (x > 0) {
          Iterator.fill(x.toInt)(t)
        } else {
          Iterator.empty
        }
      }
    }, preservesPartitioning = true)
  }

  // Sample k numbers without replacement from [0, n).
  private def sampleWithoutReplacement(n: Long, k: Int, random: RandomDataImpl): Iterable[Long] =
  {
    if (k == 0) {
      return Iterable.empty[Long]
    } else if (k < n / 3) {
      val sample = new MutableHashSet[Long]()
      // The expected number of iterations is less than 1.5*k
      while (sample.size < k) {
        sample += random.nextLong(0L, n - 1L)
      }
      return sample
    } else {
      val sample = new Array[Long](k)
      var i: Int = 0
      var j: Long = 0L
      while (j < n && i < k) {
        if (random.nextUniform(0.0, 1.0) < 1.0 * (k - i) / (n - j)) {
          sample.update(i, j)
          i += 1
        }
        j += 1L
      }
      return sample
    }
  }
}

object RDDSamplingFunctions {

  implicit def rddToRDDSamplingFunction[T: ClassTag](rdd: RDD[T]) =
    new RDDSamplingFunctions[T](rdd)
}