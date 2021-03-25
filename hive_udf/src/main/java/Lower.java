import org.apache.hadoop.hive.ql.exec.UDF;

public class Lower  extends UDF {
    //不要用static方法
    public static String evaluate(final String s) {

        if (s == null) {
            return null;
        }
        return s.toLowerCase();
    }


    public static void main(String[] args) {
        System.out.println(evaluate("DDD"));
    }
}
