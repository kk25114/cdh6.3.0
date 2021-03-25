import org.kududb.client.KuduClient;
import org.kududb.client.ListTablesResponse;

import java.util.List;





public class Demo1 {

    //列出所有的表
    public static void tableList(KuduClient kuduClient) {


        try {
            ListTablesResponse tablesList = kuduClient.getTablesList();
            List<String> tablesList1 = tablesList.getTablesList();
            for (String s : tablesList1) {
                System.out.println(s);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //不能删除表
    public static void dropTables(KuduClient kuduClient, String tableName) {

        try {

            if (kuduClient.tableExists(tableName)) {
                kuduClient.deleteTable(tableName);
                System.out.println("已经删除表");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    //不能删除表
    public static void export2csv(KuduClient kuduClient) {


    }



    public static void main(String[] args) {
        //10.9.4.89
        KuduClient kuduClient =
                new KuduClient.KuduClientBuilder("10.9.4.89:7051").build();


        //显示所有的表
        tableList(kuduClient);

        //删除表
//        dropTables(kuduClient,"kudu1.daimaku_bmk1");


        //export2csv





        try {
            kuduClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}