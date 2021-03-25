


import java.text.SimpleDateFormat;
import java.util.Date;

public class GenerateDate {

    public static String getTime() {
        Date date = new Date();
        String s = "";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String format = simpleDateFormat.format(new Date());
        s += "ods_" + format + "_r";
        return s;

    }
}