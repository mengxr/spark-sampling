package org.apache.spark.sampling

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.reflect.ClassTag
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.util.random.{XORShiftRandom, RandomSampler}
import scala.collection.mutable
import org.apache.commons.math.random.RandomDataImpl

class RDDSamplingFunctions[T: ClassTag](self: RDD[T]) extends Logging with Serializable {

  def sampleWithReplacement(s: Long, n: Long = self.count(), seed: Long = System.nanoTime()): RDD[T] = {
    val voted = new PartitionwiseSampledRDD[T, (Long, (Double, T))](self,
      new SimpleRandomSamplerWithReplacementVote[T](s, n), seed)
    voted.reduceByKey { (v1: (Double, T), v2: (Double, T)) =>
      if (v1._1 < v2._1) v1 else v2
    }.map(_._2._2)
  }

  def sampleWithoutReplacement(p: Double, n: Long = self.count(), seed: Long = System.nanoTime())
  : RDD[T] = {
    val accumulators = new SimpleRandomSamplerAccumulators[T](self.context)
    var accepted: RDD[T] = new PartitionwiseSampledRDD(self, new SimpleRandomSamplerVote[T](p, n, accumulators, seed))
    // TODO: Is it possible to describe the sampled RDD without trigger a job?
    accepted.count()
    val numItems = accumulators.numItems.value
    println(numItems)
    val numAccepted = accumulators.numAccepted.value
    println(numAccepted)
    val s = math.ceil(p * numItems).toLong
    logInfo("To sample " + s + " items, we pre-accepted " + numAccepted + ".")
    if (s > numAccepted) {
      val waitlisted = accumulators.waitlisted.value
      logInfo("To sample " + s + " items, we waitlisted " + waitlisted.size + ".")
      val waitlistAccepted = waitlisted.take((s - numAccepted).toInt).map(_._2)
      accepted = accepted.union(self.context.makeRDD(waitlistAccepted.toSeq, 1))
    }
    // TODO: how to de-register accumulators?
    accepted
  }
}

private class SimpleRandomSamplerAccumulators[T](sc: SparkContext) extends Serializable {
  val numItems = sc.accumulator[Long](0L)
  val numAccepted = sc.accumulator[Long](0L)
  val waitlisted = sc.accumulableCollection[mutable.TreeSet[(Double, T)], (Double, T)](
    new mutable.TreeSet[(Double, T)]()(Ordering.by(_._1))
  )
}

private class SimpleRandomSamplerVote[T](p: Double, n: Long, accumulators: SimpleRandomSamplerAccumulators[T], var seed: Long = System.nanoTime())
    extends RandomSampler[T, T] {

  val t1 = 20.0 / (3.0 * n)
  val q1 = p + t1 - math.sqrt(t1 * t1 + 3.0 * t1 * p)
  val t2 = 10.0 / n
  val q2 = p + t2 + math.sqrt(t2 * t2 + 2.0 * t2 * p)

  val random = new XORShiftRandom(seed)

  override def setSeed(seed: Long) {
    this.seed = seed
    random.setSeed(seed)
  }

  override def sample(items: Iterator[T]): Iterator[T] = {
    items.filter { t =>
      accumulators.numItems += 1L
      val x = random.nextDouble()
      var accepted = false
      if (x < q1) {
        accumulators.numAccepted += 1L
        accepted = true
      } else if (x < q2) {
        accumulators.waitlisted += (x, t)
      }
      accepted
    }
  }

  override def clone() = new SimpleRandomSamplerVote[T](p, n, accumulators, seed)
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

      val sample = new mutable.HashSet[Long]()

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
