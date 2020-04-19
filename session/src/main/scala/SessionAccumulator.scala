import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @author Administrator
 * @version v1.0
 * @date 2020/3/28 23:00
 * AccumulatorV2[IN, OUT]
 */
class SessionAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]]{

  val countMap = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = {
    countMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc = new SessionAccumulator
    acc.countMap ++= this.countMap
    acc
  }

  override def reset(): Unit = {
    countMap.clear()
  }

  override def add(v: String): Unit = {
    if (!this.countMap.contains(v))
      this.countMap += (v -> 0)
    this.countMap.update(v, countMap(v) + 1 )
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
        // 保证acc的累加器类型一致
      case acc: SessionAccumulator => acc.countMap.foldLeft(this.countMap){
        // this.countMap为初始值
        case (map, (k, v)) => map += (k -> (map.getOrElse(k, 0) + v))
      }

    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }
}
