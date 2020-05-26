package scala

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class sessionAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Int]] {
  val countMap=new mutable.HashMap[String,Int]();
  override def isZero: Boolean = {
    countMap.isEmpty;
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc=new sessionAccumulator;
    acc.countMap++=this.countMap;
    acc
  }

  override def reset(): Unit = {
    countMap.clear;
  }

  override def add(v: String): Unit = {
    if (!countMap.contains(v)){
      countMap+=(v->0);
    }
    countMap.update(v,countMap(v)+1);
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]])={
    other match{
      case acc:sessionAccumulator=>acc.countMap.foldLeft(this.countMap){
        case(map,(k,v))=>map+=(k->(map.getOrElse(k,0)+v));
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.countMap;
  }
}
