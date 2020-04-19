import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
 * @author Administrator
 * @version v1.0
 * @date 2020/4/5 14:32
 *
 */
class GroupConcatDistinct extends UserDefinedAggregateFunction{
  // 输入数据类型为String
  override def inputSchema: StructType = StructType(StructField("cityInfo", StringType)::Nil)
  // 缓冲区类型： StringType
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo", StringType)::Nil)
  // 输出数据类型
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  // 去重存入缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var bufferCityInfo = buffer.getString(0)
    val cityInfo = input.getString(0)

    if (!bufferCityInfo.contains(cityInfo)){
      if ("".equals(bufferCityInfo))
        bufferCityInfo += cityInfo
      else
        bufferCityInfo += "," + cityInfo
      buffer.update(0, bufferCityInfo)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // bufferCityInfo1: cityId1:cityName1, cityId2:cityName2
    var bufferCityInfo1 = buffer1.getString(0)
    // bufferCityInfo2: cityId1:cityName1, cityId2:cityName2
    val bufferCityInfo2 = buffer2.getString(0)

    for (cityInfo <- bufferCityInfo2.split(",")) {
      if (!bufferCityInfo1.contains(cityInfo)){
        if ("".equals(bufferCityInfo1))
          bufferCityInfo1 += cityInfo
        else
          bufferCityInfo1 += "," + cityInfo
      }
    }
    buffer1.update(0, bufferCityInfo1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
