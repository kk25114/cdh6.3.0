package com

object worldcount1 {

  def main(args: Array[String]): Unit = {
    var words = List("hello tom hello jerry","hello jack hello jim");
    var lines = List("hello tom hello jerry","hello jack hello jim");

//    0.对word的操作
//    val flatten = words.flatten
//    print(flatten)  //将words合并成List(h, e, l, l, o,  , t, o, m,  , h, e, l, l, o,  , j, e, r, r, y, h, e, l, l, o,  , j, a, c, k,  , h, e, l, l, o,  , j, i, m)

//    a1.lines切割
//    val  data =lines.map(_.split(" "))//按格式切割
//     a2.lines切割 -> flatten
     val flatten = lines.map(_.split(" ")).flatten
     print(flatten)
    // List(hello, tom, hello, jerry, hello, jack, hello, jim)


//    a.flarmap效果相同
//    val list = lines.flatMap(_.split(" "))
//    print(list)//List(hello, tom, hello, jerry, hello, jack, hello, jim)

    /**
      * 将list转化成map,并实现计算
      */
//      b1.使用map的参数:
//    val data = lines.map(_.split(" ")).flatten
//    print(flatten)//List(hello, tom, hello, jerry, hello, jack, hello, jim)
//    val tuples = data.map((_,1))

//    print(tuples)
//    print(tuples)//List((hello tom hello jerry,1), (hello jack hello jim,1))

//    val grouped = tuples.groupBy(_._1)
//    print(stringToTuples)//Map(hello jack hello jim -> List((hello jack hello jim,1)), hello tom hello jerry -> List((hello tom hello jerry,1)))

//    val iterable = grouped.map(_._1)
//    print(iterable)//List(hello jack hello jim, hello tom hello jerry)

//    val stringToInt = grouped.map(t => (t._1,t._2.size))
//    print(stringToInt)



  }


}
