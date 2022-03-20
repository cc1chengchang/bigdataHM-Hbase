

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseTest {
    static String MY_NAMESPACE_NAME = "chengchang";
    private static void verifyNamespaceExists(Admin admin) throws IOException {
        String namespaceName = MY_NAMESPACE_NAME;
        NamespaceDescriptor ns = NamespaceDescriptor.create(namespaceName).build();
        NamespaceDescriptor[] list = admin.listNamespaceDescriptors();
        boolean exists = false;
        for (NamespaceDescriptor nsd : list) {
            if (nsd.getName().equals(ns.getName())) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            admin.createNamespace(ns);
        }
    }

    public static void main(String[] args) throws IOException {
        // 建立连接
        System.out.println("Start connecting hbase ...");
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "emr-worker-2");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();
        System.out.println("Connected hbase");


        verifyNamespaceExists(admin);

        TableName tableName = TableName.valueOf("chengchang:student");
        String colFamily1 = "info";
        String colFamily2 = "score";
        String rowKey = "chengchang";

        // 建表
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptorInfo = new HColumnDescriptor(colFamily1);
            HColumnDescriptor hColumnDescriptorScore = new HColumnDescriptor(colFamily2);
            hTableDescriptor.addFamily(hColumnDescriptorInfo);
            hTableDescriptor.addFamily(hColumnDescriptorScore);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }
        System.out.println("Table ready");

        // 插入数据
        Put put = new Put(Bytes.toBytes(rowKey)); // row key
        put.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes("G20210698040091")); // info.col1
        put.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes("1")); // info.col2
        put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes("90")); // score.col1
        put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes("92")); // score.col2
        conn.getTable(tableName).put(put);
        System.out.println("Data insert success");

        // 查看数据
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));      // 指定rowKey
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }
}
