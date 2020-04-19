/**
 * @author Administrator
 * @version v1.0
 * @date 2020/4/4 11:54
 *
 */
case class SortKey(clickCount: Long, orderCount: Long, payCount: Long) extends Ordered[SortKey]{
  /**
   *  this.compare(that)
   *  this compare that
   *  compare > 0   this > that
   *  compare <0    this < that
   *
   * @param that
   * @return
   */
  override def compare(that: SortKey): Int = {
    if (this.clickCount - that.clickCount != 0){
      return (this.clickCount - that.clickCount).toInt
    }else if (this.orderCount - that.orderCount != 0){
      return (this.orderCount - that.orderCount).toInt
    }else{
      return (this.payCount - that.payCount).toInt
    }
  }
}
