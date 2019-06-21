import com.google.common.base.CaseFormat

/**
  * Author lzc
  * Date 2019-06-21 16:44
  */
object Test {
    def main(args: Array[String]): Unit = {
        println(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, "ab_c_abd"))  // abC
    }
}
