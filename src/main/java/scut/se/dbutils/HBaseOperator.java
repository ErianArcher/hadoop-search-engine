package scut.se.dbutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;


public class HBaseOperator {
    private Logger log = LoggerFactory.getLogger(HBaseOperator.class);

    private static HBaseOperator instance = null;

    // HBase 配置和连接实例
    private Configuration conf = null;
    private Connection conn = null;

    // 单例模式
    public synchronized static HBaseOperator getInstance() {
        if (instance == null) {
            instance = new HBaseOperator();
        }
        return instance;
    }

    private HBaseOperator() {
        initConfigAndConnection();
    }

    private void initConfigAndConnection() {
        conf = HBaseConfiguration.create();

        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            log.error("Error on getting HBase connection.", e);
        }
    }

    private Table getTable(String tableName) {
        Table t = null;
        try {
            t = conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error("Error on getting table `"+tableName+"`", e);
        }

        return t;
    }

    public <T> void insertOneRowTo(String tableName, T data, String rowKey) {
        Class dataClass = data.getClass();
        String cf = dataClass.getName();

        // Fields
        Field[] fields = dataClass.getDeclaredFields();
        List<String> cf_qualifiers = new ArrayList<>(fields.length);
        for (Field field: fields) {
            cf_qualifiers.add(field.getName());
        }

        // Retrieve all methods which are for get fields.
        Map<String, Method> getMethods = Arrays.stream(dataClass.getMethods())
                .filter((m) -> m.getName().contains("get"))
                .filter(m -> m.getParameterCount() == 0)  // No parameters
                .flatMap(gm -> {
                    Map<String, Method> mapping = new HashMap<>(1);
                    mapping.put(gm.getName(), gm);
                    return mapping.entrySet().stream();
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Get datum of fields
        Map<String, String> fieldName2Data = new HashMap<>(cf_qualifiers.size());
        cf_qualifiers.forEach(cf_qualifier -> {
            StringBuffer sb = new StringBuffer();
            sb.append("get");
            sb.append(Character.toUpperCase(cf_qualifier.substring(0, 1).toCharArray()[0]));
            sb.append(cf_qualifier.substring(1));
            String getMethodName = sb.toString();

            Method getCf_qualifierData = getMethods.get(getMethodName);
            if (getCf_qualifierData == null) return ; // 没找到该方法时跳过

            String cf_qfData = null;
            try {
               cf_qfData  = getCf_qualifierData.invoke(data).toString();
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
            if (cf_qfData == null) cf_qfData = "";
            fieldName2Data.put(cf_qualifier, cf_qfData);
        });

        // Write row data
        Table table = getTable(tableName);

        Put p = new Put(hashRowKey(rowKey));

        for (Map.Entry<String, String> entry : fieldName2Data.entrySet()) {
            p.addColumn(Bytes.toBytes(cf),
                    Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
        }

        // 把数据写入HBase
        try {
            table.put(p);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(null, null, table);
        }
    }

    public void setColumnValue(String tableName, String rowKey, String familyName, String column1, String value1){
        Table table=null;
        try {
            // 获取表
            table=getTable(tableName);
            // 设置rowKey
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column1), Bytes.toBytes(value1));

            table.put(put);
            log.debug("add data Success!");
        }catch (IOException e) {
            log.error(MessageFormat.format("为表的某个单元格赋值失败,tableName:{0},rowKey:{1},familyName:{2},column:{3}"
                    ,tableName,rowKey,familyName,column1),e);
        }finally {
            close(null,null,table);
        }
    }

    public Map<String,Map<String,String>> getResultScanner(String tableName){
        Scan scan = new Scan();
        return this.queryData(tableName,scan);
    }

    /**
     * 匹配列的值等于`keyword`
     * @param tableName 表格名
     * @param columnFamily 列族名
     * @param column 列名
     * @param keyword 关键词
     * @return 所有value 等于keyword 的(rowKey, (qualifier, value))
     */
    public Map<String, Map<String, String>> getColValWithKeyword(String tableName, String columnFamily, String column,
                                                                         String keyword) {
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                CompareOperator.EQUAL, Bytes.toBytes(keyword));  // 子串匹配
        scan.setFilter(filter);
        return this.queryData(tableName, scan);
    }

    /**
     * 匹配列的值其中`keyword`是其子串
     * @param tableName 表格名
     * @param columnFamily 列族名
     * @param column 列名
     * @param keyword 关键词
     * @return 所有包含keyword 的(rowKey, (qualifier, value))
     */
    public Map<String, Map<String, String>> getColValWithKeywordInSubStr(String tableName, String columnFamily, String column,
                                                                         String keyword) {
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                CompareOperator.EQUAL, new SubstringComparator(keyword));  // 子串匹配
        scan.setFilter(filter);
        return this.queryData(tableName, scan);
    }

    /**
     * 根据Scanner 查询所有数据
     * @param tableName 表格名
     * @param scan Scanner 可以带有过滤规则
     * @return
     */
    private Map<String, Map<String, String>> queryData(String tableName, Scan scan) {
        Map<String, Map<String, String>> result = new HashMap<>();

        ResultScanner rs = null;
        Table table = null;

        try {
            table = getTable(tableName);
            rs = table.getScanner(scan);
            for (Result r : rs) {
                //每一行数据
                Map<String,String> columnMap = new HashMap<>();
                String rowKey = null;
                for (Cell cell : r.listCells()) {
                    if(rowKey == null){
                        rowKey = bytes2String(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
                    }
                    columnMap.put(bytes2String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                            bytes2String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }

                if(rowKey != null){
                    result.put(rowKey,columnMap);
                }
            }
        }catch (IOException e) {
            log.error(MessageFormat.format("遍历查询指定表中的所有数据失败,tableName:{0}"
                    ,tableName),e);
        }finally {
            close(null,rs,table);
        }

        return result;
    }

    /**
     * 查询某个列键的值
     * @param tableName 表格名
     * @param rowKey 行键
     * @return value
     */
    public Map<String,String> getRowData(String tableName, String rowKey){
        //返回的键值对
        Map<String,String> result = new HashMap<>();

        Get get = new Get(hashRowKey(rowKey));
        // 获取表
        Table table= null;
        try {
            table = getTable(tableName);
            Result hTableResult = table.get(get);
            if (hTableResult != null && !hTableResult.isEmpty()) {
                for (Cell cell : hTableResult.listCells()) {
                    result.put(bytes2String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                            , bytes2String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }
        }catch (IOException e) {
            log.error(MessageFormat.format("查询一行的数据失败,tableName:{0},rowKey:{1}"
                    ,tableName,rowKey),e);
        }finally {
            close(null,null,table);
        }

        return result;
    }

    /**
     * 根据行键、列族和列名查询值
     * @param tableName 表格名
     * @param rowKey 行键
     * @param familyName 列族名
     * @param columnName 列名
     * @return value
     */
    public String getColumnValue(String tableName, String rowKey, String familyName, String columnName){
        String str = null;
        Get get = new Get(hashRowKey(rowKey));
        // 获取表
        Table table= null;
        try {
            table = getTable(tableName);
            Result result = table.get(get);
            if (result != null && !result.isEmpty()) {
                Cell cell = result.getColumnLatestCell(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
                if(cell != null){
                    str = bytes2String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
            }
        } catch (IOException e) {
            log.error(MessageFormat.format("查询指定单元格的数据失败,tableName:{0},rowKey:{1},familyName:{2},columnName:{3}"
                    ,tableName,rowKey,familyName,columnName),e);
        }finally {
            close(null,null,table);
        }

        return str;
    }

    public boolean createTable(String tableName, List<String> columnFamily) {
        Admin admin = null;
        try {
            admin = conn.getAdmin();

            List<ColumnFamilyDescriptor> familyDescriptors = new ArrayList<>(columnFamily.size());

            columnFamily.forEach(cf -> {
                familyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build());
            });

            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    .setColumnFamilies(familyDescriptors)
                    .build();

            if (admin.tableExists(TableName.valueOf(tableName))) {
                log.debug("table Exists!");
            } else {
                admin.createTable(tableDescriptor);
                log.debug("create table Success!");
            }
        } catch (IOException e) {
            log.error(MessageFormat.format("创建表{0}失败", tableName), e);
            return false;
        } finally {
            close(admin, null, null);
        }

        return true;
    }

    public boolean deleteTable(String tableName){
        Admin admin = null;
        try {
            admin = conn.getAdmin();

            if(admin.tableExists(TableName.valueOf(tableName))){
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
                log.debug(tableName + "is deleted!");
            }
        }catch (IOException e) {
            log.error(MessageFormat.format("删除指定的表失败,tableName:{0}"
                    ,tableName),e);
            return false;
        }finally {
            close(admin,null,null);
        }
        return true;
    }

    private byte[] hashRowKey(String rowKey) {
        return Bytes.toBytes(rowKey); // TODO: Transform `rowKey` into hash code.
    }

    /**
     * 解码Cell 返回的数据
     * @param bytes 原字节数组
     * @param start 数据起始点
     * @param length 数据长度
     * @return 数据的字符串
     */
    private String bytes2String(byte[] bytes, int start, int length) {
        return Bytes.toString(Arrays.copyOfRange(bytes, start, start+length));
    }

    /**
     * 关闭流
     */
    private void close(Admin admin, ResultScanner rs, Table table){
        if(admin != null){
            try {
                admin.close();
            } catch (IOException e) {
                log.error("关闭Admin失败",e);
            }
        }

        if(rs != null){
            rs.close();
        }

        if(table != null){
            try {
                table.close();
            } catch (IOException e) {
                log.error("Error on closing table `"+table.getName()+"`", e);
            }
        }
    }
}
