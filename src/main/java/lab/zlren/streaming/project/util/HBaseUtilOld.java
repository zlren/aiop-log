package lab.zlren.streaming.project.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase操作工具类
 * 单例模式
 *
 * @author zlren
 * @date 2017-12-22
 */
public class HBaseUtilOld {

    private HBaseAdmin admin = null;
    private Configuration conf = null;

    private HBaseUtilOld() {

        // 这些参数的配置在HBASE_HOME/conf/hbase-site.xml文件中
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "10.109.246.67:2181");
        conf.set("hbase.rootdir", "hdfs://10.109.246.67:9000/hbase");

        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtilOld instance = null;

    public static synchronized HBaseUtilOld getInstance() {
        if (null == instance) {
            instance = new HBaseUtilOld();
        }
        return instance;
    }


    /**
     * 根据表表名获取一张表
     *
     * @param tableName 表名
     * @return
     */
    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(conf, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }


    /**
     * 添加一条记录
     *
     * @param tableName 表名
     * @param rowKey    行健
     * @param cf        列族
     * @param value     值
     */
    public void put(String tableName, String rowKey, String cf, String column, String value) {
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        HBaseUtilOld.getInstance().put("aiop_log", "20171111_131", "info", "level", String.valueOf(99));
    }
}
