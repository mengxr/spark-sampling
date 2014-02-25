package org.apache.spark.sampling

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.util.random.RandomSampler
import scala.collection.mutable.{ArrayBuffer, HashSet => MutableHashSet}
import org.apache.commons.math.random.RandomDataImpl
import java.util.Random

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
    val voted = new PartitionwiseSampledRDD[T, (Long, (Double, T))](self,
      new SimpleRandomSamplerWithReplacementVote[T](s, n), seed)
    voted.reduceByKey { (v1: (Double, T), v2: (Double, T)) =>
      if (v1._1 < v2._1) v1 else v2
    }.map(_._2._2)
  }
}

private class SimpleRandomSamplerWithReplacementVote[T](s: Long, n: Long,
    var seed: Long = System.nanoTime()) extends RandomSampler[T, (Long, (Double, T))] {

  val random = new RandomDataImpl()
  random.reSeed(seed)

  val failureRate = 1e-4
  val threshold = 1.0 - math.exp(math.log(failureRate / s) / n);

  def setSeed(seed: Long) {
    this.seed = seed
    random.reSeed(seed)
  }

  def sample(items: Iterator[T]): Iterator[(Long, (Double, T))] = {
    items.map { t =>
      (random.nextBinomial(s.toInt, threshold), t)
    }.filter(_._1 > 0).flatMap { case (i, t) =>
      sampleWithoutReplacement(s, i).map { pos =>
        (pos, (random.nextUniform(0.0, 1.0), t))
      }
    }
  }

  override def clone() = new SimpleRandomSamplerWithReplacementVote[T](s, n, seed)

  private def sampleWithoutReplacement(n: Long, k: Int): Iterable[Long] =
  {
    if (k == 0) {
      return Iterable.empty[Long]
    }

    if (k < n / 3) {

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
