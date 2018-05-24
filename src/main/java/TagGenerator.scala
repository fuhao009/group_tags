import org.apache.spark.{SparkConf, SparkContext}
object TagGenerator {
  def main(args: Array[String])= {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("tagGen")
    val sc = new SparkContext(conf)
//    val sqlContext = new HiveContext(sc)
//    import sqlContext.implicits._
    //设置检查点
    val rdd1 = sc.textFile("hdfs://192.168.16.194:9000/spark/temptags.txt") // to be replaced by hive data resources
    // 77287793  高大上","环境优雅","价格实惠
    val rdd2 = rdd1.map(e=>e.split("\t")).filter(e=>e.length == 2)
      // key -> value ==> (key,value), key 是店铺ID  value 是通过ReviewTags.extractTags函数抽取日志中的标签如：环境优雅,高大上,价格实惠
    val rdd3 = rdd2.map(e=>e(0)->ReviewTags.extractTags(e(1)))
      // {}集合中的第二个元素 = e._2,判断是否有值,返回(77287793,["环境优雅","性价比高","干净卫生","停车方便","音响效果好"])
      //  ArrayBuffer((86913510,[Ljava.lang.String;@713064e8), (77287793,[Ljava.lang.String;@4fad6218)
      // [ 代表一维数组, L 代表 对象类型
   val rdd4 = rdd3.filter(e => e._2.length > 0).map(e=> e._1 -> e._2.split(","))
      // 将对象数组转(店铺id,关键字)
      // ArrayBuffer((86913510,午餐), (86913510,分量适中), (77287793,干净卫生), (77287793,服务热情), (77287793,音响效果好)
   val rdd5 = rdd4.flatMapValues(e=>e)
      // ArrayBuffer(((86913510,午餐),1), ((86913510,分量适中),1), ((77287793,干净卫生),1), ((77287793,服务热情),1)
      // (86913510,午餐) 转成 ((86913510,午餐),1)
    val rdd6 = rdd5.map(e=> (e._1,e._2) -> 1)
//       根据key计数
      // ArrayBuffer(((85648235,体验好),9), ((77373671,菜品差),1), ((84270191,干净卫生),1), ((73607905,分量足),13),可以指定分区数
    val rdd7 = rdd6.reduceByKey(_+_,3)
      // ((85648235,体验好),9) 取出列表的第一个元素(85648235,体验好)中的第二个元素和列表中的第二个元素组合返回一个((体验好,9)并且转成列表格式
      // ArrayBuffer((85648235,List((体验好,9))), (77373671,List((菜品差,1)))
      // list 可以聚合,元祖无法聚合
     val rdd8 = rdd7.map(e=> e._1._1 -> List((e._1._2,e._2)))
      // ::: 多个list合并到一个list
      // 因为value是列无法累计计算,只能使用:::合并到到一个list中
      // (83644298,List((体验好,1), (性价比高,1), (服务热情,1), (价格实惠,1), (味道赞,1))), (82317795,List((味道差,1))
    val rdd9 = rdd8.reduceByKey(_ ::: _,3)
      // 返回一个(key,value),其中key就是店铺id,主要是针对value进行操作
      // sortBy(_._2)： 针对第二列进行倒序(reverse)排序,reverse.take(10)：只显示前面10条,map(a=> a._1+":"+a._2.toString).mkString(",")：将list中的第一个和第二个元素提取出来合并成一个(key:value), mkString(",")：由于value是list格式,需要将list转str,并且以逗号分隔最后结果是 (id,服务热情,20,服务态度,10,……)
    val rdd10 = rdd9.map(e=> e._1-> e._2.sortBy(_._2).reverse.take(10).map(a=> a._1+":"+a._2.toString).mkString(","))
      // 输出到hdfs中,输出的目录必须是不存在的
    rdd10.map(e=>e._1+"\t"+e._2).saveAsTextFile("hdfs://192.168.16.194:9000/spark_out2")
      // 停止
    sc.stop()
  }
}
