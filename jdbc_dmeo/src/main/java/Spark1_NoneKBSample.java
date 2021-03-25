import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


//不支持
public class Spark1_NoneKBSample {

    private static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static String CONNECTION_URL ="jdbc:hive2://192.168.50.135:10000/";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        System.out.println("使用JDBC的方式访问非Kerberos环境下Spark1.6 Thrift Server");
        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            connection = DriverManager.getConnection(CONNECTION_URL, "hive", "");
            ps = connection.prepareStatement("select * from test");
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