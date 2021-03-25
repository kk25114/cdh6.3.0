import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


public class Impala_NoneKBSimple {

    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    private static String CONNECTION_URL ="jdbc:impala://10.214.22.46:21050/";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (Exception e) {
            System.out.println("找不到驱动程序类 ，加载驱动失败！");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("通过JDBC连接非Kerberos环境下的Impala");
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
