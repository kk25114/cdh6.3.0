

import java.sql.*;
import java.util.*;

public class BmkController {
    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
//    private static String CONNECTION_URL = "jdbc:impala://192.168.50.135:21050/";
    private static String CONNECTION_URL = "jdbc:impala://10.214.22.46:21050/";
    //    private static String CONNECTION_URL = "jdbc:impala://10.9.4.89:21050/";
    private static final String SQL_STATEMENT = "";
    private static Connection con = null;
    private static ResultSet rs = null;

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (Exception e) {
            System.out.println("缺少impalajdbc41!");
            e.printStackTrace();
        }
    }

    //0.create
    public static void create(Statement stmt) {
        //对表进行处理
        String sql = "DESCRIBE kudu1.daimaku_bmk";
        Map maps = new LinkedHashMap();

        StringBuffer sb = new StringBuffer("create table ");
        String s1 = "";

        try {
            //表中的字段
            ResultSet rs = stmt.executeQuery(sql.toString());
            while (rs.next()) {
                maps.put(rs.getString(1), rs.getString(2));
            }
            //取map中的字段拼接
            Iterator it = maps.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry entity = (Map.Entry) it.next();
                s1 += entity.getKey() + " " + entity.getValue() + ",";
            }
            String str = s1.substring(0, s1.lastIndexOf(","));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    //创建表
    public static void create_test(Statement stmt) {
        //对表进行处理
        String sql = "show create table kudu1.daimaku_bmk";
        String s1 = "";
        String s2 = "";
        StringBuffer sb = new StringBuffer("create table ");
        String table_name = GenerateDate.getTime();
        sb.append(table_name);


        //表中的字段
        try {
            ResultSet rs = stmt.executeQuery(sql.toString());
            while (rs.next()) {
                s1 += rs.getString(1);
            }
            s2 = s1.substring(s1.indexOf("("));
            sb.append(s2);
            stmt.execute(sb.toString());

        } catch (SQLException e) {
            e.printStackTrace();

        }
    }


    //1.insert
    public static void insert(Statement stmt) {
        String sql = " insert  into kudu1.daimaku_bmk values (100,\"测试1\",\"测试1\",\"测试1\",\"测试1\",\"测试1\",\"测试1\",\"测试1\")";
        try {
            stmt.execute(sql);
            System.out.println("is inserted");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //1.1 upsert
    public static void upsert(Statement stmt) {


    }

    //2.delete
    public static void delete(Statement stmt) {
        String sql = " delete  from  kudu1.daimaku_bmk where id=100";
        try {
            stmt.execute(sql);
            System.out.println("is deleted");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    //2.update
    public static void update(Statement stmt) {
        String sql = " update  kudu1.daimaku_bmk set schoolname=\"测试2\" where id=100";
        try {
            stmt.execute(sql);
            System.out.println("is inserted");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    //1.select
    public static ResultSet select(Statement stmt, String table1) {
        StringBuffer sql = new StringBuffer();
        sql.append("select * from ");
        sql.append(table1);
        try {
            return stmt.executeQuery(sql.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }


    //2.多表
    public static ResultSet select_splice(Statement stmt, String[] table1, String[] table2) {
//        ArrayList<Object> params = new ArrayList<>();
        for (String s : table1) {

        }
        //合并表


        StringBuffer sql = new StringBuffer();
        sql.append("select * from ");
        sql.append(table1);
        sql.append("left join");
        sql.append(table2);
        sql.append("left join");


        try {
            return stmt.executeQuery(sql.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }


    public static void main(String[] args) {
        System.out.println("通过JDBC连接非Kerberos环境下的Impala");
        try {
            //1.get  connection
            con = DriverManager.getConnection(CONNECTION_URL);
            Statement stmt = con.createStatement();


//            ResultSet rs = selectAll(stmt);
//            insert(stmt);
//            ResultSet rs = select_splice(stmt, "kudu1.daimaku_bmk");
//            insert(stmt);
//            update(stmt);
//            delete(stmt);

            create_test(stmt);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}