
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.ListTablesResponse;


import java.util.List;

//可以使用的
public class KuduExample {

    //    private static final String KUDU_MASTER = System.getProperty("kuduMasters", "cdh1.fayson.com:7051,cdh2.fayson.com:7051,cdh3.fayson.com:7051");
//        private static final String KUDU_MASTER = System.getProperty("kuduMasters", "192.168.50.135:7051");
    private static final String KUDU_MASTER = System.getProperty("kuduMasters", "10.9.4.89:7051");

    public static void main(String[] args) {
        KuduClient kuduClient = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
        String tableName = "user_info3";
        System.out.println(kuduClient);

        //删除Kudu的表
//        KuduUtils.dropTable(kuduClient, tableName);

        //创建一个Kudu的表
        KuduUtils.createTable(kuduClient, tableName);

        //列出Kudu下所有的表
//        KuduUtils.tableList(kuduClient);

        //向Kudu指定的表中插入数据
//        KuduUtils.upsert(kuduClient, tableName, 100);

        //扫描Kudu表中数据
//        KuduUtils.scanerTable(kuduClient, tableName);

        try {
            kuduClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
