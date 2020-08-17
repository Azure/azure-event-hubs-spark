package org.apache.spark.eventhubs

package object client {
  implicit def toJavaFunction[A, B](f: Function1[A, B]) = new java.util.function.Function[A, B] {
    override def apply(a: A): B = f(a)
  }
}
