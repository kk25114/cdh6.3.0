import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


public class Hive_NoneKBSimple {
    private static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static String CONNECTION_URL ="jdbc:hive2://10.214.22.46:10000/";
//    private static String CONNECTION_URL ="jdbc:hive2://192.168.50.135:10000/";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("通过JDBC连接非Kerberos环境下的HiveServer2");
        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            connection = DriverManager.getConnection(CONNECTION_URL);
            ps = connection.prepareStatement("select * from yanke_data.dept");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getInt(1) + "-------" + rs.getString(2));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.disconnect(connection, rs, ps);
        }
    }
}
