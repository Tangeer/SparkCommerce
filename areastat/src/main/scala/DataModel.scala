/**
 * @author Administrator
 * @version v1.0
 * @date 2020/4/5 12:51
 *
 */
case class CityClickProduct(city_id: Long,
                            click_product_id: Long)

case class CityAreaInfo(city_id:Long,
                        city_name:String,
                        area:String)

//***************** 输出表 *********************

/**
 *
 * @param taskid
 * @param area
 * @param areaLevel
 * @param productid
 * @param cityInfos
 * @param clickCount
 * @param productName
 * @param productStatus
 */
case class AreaTop3Product(taskId:String,
                           area:String,
                           areaLevel:String,
                           productId:Long,
                           cityInfos:String,
                           clickCount:Long,
                           productName:String,
                           productStatus:String)
