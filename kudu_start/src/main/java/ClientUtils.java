import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import java.util.Properties;

public class ClientUtils {

    /**
     * 初始化访问Kerberos访问
     * @param debug 是否启用Kerberos的Debug模式
     */
    public static void initKerberosENV(Boolean debug) {
        try {
            Properties properties = new Properties();
            properties.load(ClientUtils.class.getClass().getResourceAsStream("/client.properties"));

            System.setProperty("java.security.krb5.conf", properties.getProperty("krb5.conf.path"));
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            if (debug) System.setProperty("sun.security.krb5.debug", "true");

            Configuration configuration = new Configuration();
            configuration.addResource(ClientUtils.class.getClass().getResourceAsStream("/hdfs-client-kb/core-site.xml"));
            configuration.addResource(ClientUtils.class.getClass().getResourceAsStream("/hdfs-client-kb/hdfs-site.xml"));
            UserGroupInformation.setConfiguration(configuration);

            UserGroupInformation.loginUserFromKeytab(properties.getProperty("kerberos.user"), properties.getProperty("kerberos.keytab.path"));
            System.out.println(UserGroupInformation.getCurrentUser());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
