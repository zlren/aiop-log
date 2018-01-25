package lab.zlren.streaming.aiop.log.util;

import lab.zlren.streaming.aiop.log.entity.HBaseData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @author zlren
 * @date 2018-01-19
 */
public class HBaseUtil {

    private static Connection connection = null;
    private static HBaseUtil instance = null;

    private HBaseUtil() {

        // 这些参数的配置在HBASE_HOME/conf/hbase-site.xml文件中
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "10.109.246.67:2181");
            conf.set("hbase.rootdir", "hdfs://10.109.246.67:9000/hbase");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static synchronized HBaseUtil getInstance() {
        if (null == instance) {
            instance = new HBaseUtil();
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
            table = (HTable) connection.getTable(TableName.valueOf(tableName));
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
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量插入一行的多列数据
     *
     * @param tableName 表名
     * @param rowKey    行健
     * @param dataList  数据
     */
    public void putList(String tableName, String rowKey, List<HBaseData> dataList) {

        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        dataList.forEach(data -> {
            if (data.value().length() > 0) {
                put.addColumn(
                        Bytes.toBytes(data.columnFamily()),
                        Bytes.toBytes(data.column()),
                        Bytes.toBytes(data.value())
                );
            }
        });

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
