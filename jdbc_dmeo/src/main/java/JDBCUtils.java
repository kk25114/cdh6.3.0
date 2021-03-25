import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class JDBCUtils {

    /**
     * 关闭数据库连接
     * @param connection
     * @param res
     * @param ps
     * @throws SQLException
     */
    public static void disconnect(Connection connection, ResultSet res, PreparedStatement ps) {

        try {
            if (res != null)  res.close();
            if (ps != null) ps.close();
            if (connection != null) connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
