package org.apache.spark.sampling

import org.apache.commons.math3.distribution.PoissonDistribution
import org.apache.commons.math3.distribution.NormalDistribution

object PoissonBounds {

  val delta = 1e-4 / 3.0
  val phi = new NormalDistribution().cumulativeProbability(1.0 - delta)

  def getLambda1(s: Double): Double = {
    var lb = math.max(0.0, s - math.sqrt(s / delta)) // Chebyshev's inequality
    var ub = s
    while (lb < ub - 1.0) {
      val m = (lb + ub) / 2.0
      val poisson = new PoissonDistribution(m, 1e-15)
      val y = poisson.inverseCumulativeProbability(1 - delta)      if (y > s) ub = m else lb = m
    }
    lb
  }

  def getMinCount(lmbd: Double): Double = {
    if(lmbd == 0) return 0
    val poisson = new PoissonDistribution(lmbd, 1e-15)
    poisson.inverseCumulativeProbability(delta)
  }

  def getLambda2(s: Double): Double = {
    var lb = s
    var ub = s + math.sqrt(s / delta) // Chebyshev's inequality
    while (lb < ub - 1.0) {
      val m = (lb + ub) / 2.0
      val poisson = new PoissonDistribution(m, 1e-15)
      val y = poisson.inverseCumulativeProbability(delta)
      if (y >= s) ub = m else lb = m
    }
    ub
  }
}

object PoissonBoundsTest extends App {

  val s = 1000000
  val lmbd1 = PoissonBounds.getLambda1(s)
  val lb = PoissonBounds.getMinCount(lmbd1)
  val lmbd2 = PoissonBounds.getLambda2(s - lb)

  println(List(lmbd1, lb, lmbd2))
}