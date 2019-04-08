package com.main.java;

import cn.bdp.bean.Field;
import cn.bdp.bean.Schema;
import cn.bdp.bean.TableInfo;
import cn.bdp.sdk.BDPClient;
import cn.bdp.sdk.DS;
import cn.bdp.sdk.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by 肖英峰--Hansell on 2017/4/15.
 */
public class DataOperator {
    //private static final String base="http://192.168.41.86:2470";
    //f90c063f188d5698b2af62e1075d8a72  token of xiaoyingfeng
    //403a0ec12806136be95ab1e32c797332 token of admin
    private  String token ="f90c063f188d5698b2af62e1075d8a72";
    private  String dataSource ="";
    private  String tableName ="";
    private  BDPClient client = new BDPClient(token);
    private  DS ds = null;
    private  Table table = null;
    private  String[] fields;
    public  String getTableName() {
        return tableName;
    }
    public  void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public  String getDataSource() {
        return dataSource;
    }

    public  void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    {
        System.out.println("--bdp client ini---");
        BDPClient client = new BDPClient(token);

    }
    public void init(){
        System.out.println("--data_source is "+dataSource+",table is "+tableName);
        ds = client.getDs(dataSource);
        table = ds.getTable(tableName);
        fields=getFields("1","2");
    }
    public void initDs(){
        System.out.println("--data_source is "+dataSource+",table is "+tableName);
        ds = client.getDs(dataSource);
    }
    public  void billdateBulkLike(String conditions) {
        //String where = " `OPT_GGS` like '%insert%' ";
        String where = "`BILL_DATE`  like '%"+conditions+"%'";
        //String where = "`id`=1";
        table.bulkDelete(where);
        commit();
        String res=getInfo();
        System.out.println(res);
    }

    public  void bulkDeleteCommit(String conditions) {

        //String where = " `OPT_GGS` like '%insert%' ";
       // String where = "`OPT_GGS` like '%"+conditions+"%'";
        String where = "`OPT_GGS` <= '"+conditions+"'";
        //String where = "`id`=1";
        table.bulkDelete(where);
        commit();
        String res=getInfo();
        System.out.println(res);
    }
    public  void commitTimeBulkDel(String conditions) {
        String where = "`COMMIT_TIME` like '%"+conditions+"%'";
        table.bulkDelete(where);
        commit();
        String res=getInfo();
        System.out.println(res);
    }
    public  void UuIdEqual(String conditions) {
        //String where = " `OPT_GGS` like '%insert%' ";
        String where = "`UU_ID` = '"+conditions+"'";
        //String where = "`id`=1";
        table.bulkDelete(where);
        commit();
        String res=getInfo();
        System.out.println(res);
    }
    public  void optimeBulkEqual(String conditions) {
        //String where = " `OPT_GGS` like '%insert%' ";
        String where = "`OPTIME_GGS` = '"+conditions+"'";
        //String where = "`id`=1";
        table.bulkDelete(where);
        commit();
        String res=getInfo();
        System.out.println(res);
    }
    public  void optimeBulkLike(String conditions) {
        //String where = " `OPT_GGS` like '%insert%' ";
        String where = "`OPTIME_GGS`  like '%"+conditions+"%'";
        //String where = "`id`=1";
        table.bulkDelete(where);
        commit();
        String res=getInfo();
        System.out.println("del table name : " + res);
    }
    public  void optimeBulkLikeNot(String conditions) {
        Table table = ds.getTable(tableName);
        //String where = " `OPT_GGS` like '%insert%' ";
        String where = "`OPTIME_GGS` not like '%"+conditions+"%'";
        //String where = "`id`=1";
        table.bulkDelete(where);
        commit();
        String res=getInfo();
        System.out.println(res);
    }
    public  void selfBulkLike(String selfStr, String conditions) {
        //String where = " `OPT_GGS` like '%insert%' ";
        String where = "`" + selfStr + "` like '%"+conditions+"%'";
        //String where = "`id`=1";
        table.bulkDelete(where);
        commit();
        String res=getInfo();
        System.out.println(res);
    }
    public  void createTableQC(String tableName,String str) {
        List<Schema> schemas = new ArrayList<Schema>();
        String[] types=str.split("\\|");
        for(String s:types){
            Schema ss=new Schema(s,"string");
            schemas.add(ss);
        }
        Table table = ds.createTable(tableName.toUpperCase()+"_HISTORY", schemas, null, (String)null);
        System.out.println(table);
    }
    public  void createTableNew(String tableName,String str) {
        List<Schema> schemas = new ArrayList<Schema>();
        String[] types=str.split("\\|");
        for(String s:types){
            Schema ss=new Schema(s,"string");
            schemas.add(ss);
        }
        Table table = ds.createTable(tableName.toUpperCase(), schemas, null, (String)null);
        System.out.println(table);
    }
    public  void createTableHistory(String tableName,String str) {
        List<Schema> schemas = new ArrayList<Schema>();
        String[] types=str.split("\\|");
        for(String s:types){
            Schema ss=new Schema(s,"string");
            schemas.add(ss);
        }
        Schema schema1 = new Schema("OPT_GGS", "string");
        Schema schema2 = new Schema("VALID_GGS", "string");
        Schema schema3 = new Schema("OPTIME_GGS", "string");
        Schema schema4 = new Schema("COMMIT_TIME", "string");
        Schema schema5 = new Schema("UU_ID", "string");
        schemas.add(schema1);
        schemas.add(schema2);
        schemas.add(schema3);
        schemas.add(schema4);
        schemas.add(schema5);
        String[] uniqKey = new String[]{"UU_ID"};
        Table table = ds.createTable(tableName.toUpperCase()+"_HISTORY", schemas, uniqKey, (String)null);
        System.out.println(table);
    }
    public  void createTableHistorySpecial(String tableName,String str) {
        //BDPClient client = new BDPClient(token);
        //DS ds = client.getDs(dss);
        List<Schema> schemas = new ArrayList<Schema>();
        String[] types=str.split("\\|");
        for(String s:types){
            Schema ss=new Schema(s,"string");
            schemas.add(ss);
        }
        Schema schema4 = new Schema("COMMIT_TIME", "string");
        Schema schema5 = new Schema("UU_ID", "string");
        schemas.add(schema4);
        schemas.add(schema5);
        String[] uniqKey = new String[]{"UU_ID"};
        Table table = ds.createTable(tableName.toUpperCase()+"_HISTORY", schemas, uniqKey, (String)null);
        System.out.println(table);
    }
    //private static String tbId="tb_2fe7a02698b9421a92b320f11577611e";

    public  void insertData(String[][] data) throws InterruptedException {
        insertDataByNameed(fields,data);
        Thread.sleep(1000);
        // commit();
    }

    private  void bulkDelete(String time) {
        //BDPClient client = new BDPClient(token);
       // DS ds = client.getDs(dataSource);
       // Table table = ds.getTable(tableName);
        String where = "`committime` like '%"+time+"%'";
        table.bulkDelete(where);
    }
    private  void insertTestNameed(String[] fields,String[][] data) {
        //BDPClient client = new BDPClient(token);
        //DS ds = client.getDs(dss);
        System.out.println(fields.length);
        for(int i=0;i<fields.length;i++) {
            System.out.print(fields[i]+"---");
        }
        table.insertDataByName(fields,data);
    }
    private  void insertDataByNameed(String[] fields,String[][] data) {
        //BDPClient client = new BDPClient(token);
        //DS ds = client.getDs(dss);
        //Table table = ds.getTable(tables);
       /* for(int i=0;i<fields.length;i++) {
            System.out.print(fields[i]);
            System.out.println("");
            System.out.println("");
        }
       System.out.println(table);*/
       System.out.println(data.length);
        table.insertDataByName(fields,data);
    }
    private  void insertDataByArr(String[] fields,String data[]) {
       // BDPClient client = new BDPClient(token);
        DS ds = client.getDs(dataSource);
        Table table = ds.getTable(tableName);
        String[][] arr = new String[1][3];
        for(int i=0;i<arr.length;i++){
            for(int j=0;j<arr[i].length;j++){
                for(int x=0;x<data.length;x++)
                    arr[i][j]=data[5*i+j];
            }
        }

        for(int i=0;i<arr.length;i++){
            System.out.println();
            for(int j=0;j<arr[i].length;j++){
                System.out.print(arr[i][j]+",");
            }
        }
        table.insertDataByName(fields,arr);
    }
    public  void addField(String datas,String tName){
       // BDPClient client = new BDPClient(token);
        DS ds = client.getDs(datas);
        Table table = ds.getTable(tName);
        table.addField("OPT_GGS", "string", "0", "opt ggs");
        table.addField("VALID_GGS", "string", "0", "valid ggs");
        table.addField("OPTIME_GGS", "string", "0", "optime ggs");
    }
    private  void deleteDs() {
       // BDPClient client = new BDPClient(token);
        client.deleteDs("kongchao3");
    }
    private  void addField(String token,String dataSource,String tableName) {
       // BDPClient client = new BDPClient(token);
        DS ds = client.getDs(dataSource);
        Table table = ds.getTable(tableName);
        table.addField("newField", "string", "1", "aliasField");
    }
    public void updateAll() {
        ds.updateAll();
    }
    public void updateTable(String tbName) {
        String[] tables = new String[]{tbName};
        ds.update(tables);
        System.out.println(tbName +" has already updated!");
    }

   /* public  void updateDataByName(String tableName) {
        //BDPClient client = new BDPClient(token);
        //DS ds = client.getDs(dataSource);
        Table table = ds.getTable(tableName);
        //table.updateDataByName(this.fields, this.datas);
    }*/
    private  void listAllTable() {
        //BDPClient client = new BDPClient(token);
        DS ds = client.getDs("ds_tw");
        Map<String, Table> tables = ds.getAllTables();
        System.out.println(tables);
    }
    public  String createDs(String datas) {
       // BDPClient client = new BDPClient(token);
        client.createDs(datas);
        return "success";
    }
    public  String getDs(String datas) {
       // BDPClient client = new BDPClient(token);
        DS ds = client.getDs(datas);
        return ds.getDsId()+"---------"+ds.toString();
    }
    public  void deleteDs(String datas) {
        client.deleteDs(datas);
    }
    public  void deleteTb(String tableName) {
        ds.deleteTable(tableName);
    }
    private  void createTable() {
       // BDPClient client = new BDPClient(token);
        DS ds = client.getDs(dataSource);
        List<Schema> schemas = new ArrayList();
        Schema schema1 = new Schema("ids", "string");
        Schema schema2 = new Schema("name", "string");
        Schema schema3 = new Schema("birth", "string");
        schemas.add(schema1);
        schemas.add(schema2);
        schemas.add(schema3);
        String[] uniqKey = new String[]{"ids"};
        Table table = ds.createTable(tableName, schemas, uniqKey, (String)null);
        System.out.println(table);
    }
    public  void commit() {
        table.commit();
    }
    public  void clean(String datas,String tableNames) {
        table.clean();
        commit();
    }
    public  String getInfo() {
        TableInfo tableInfo = table.getInfo();
        return tableInfo.name;
        // return tableInfo.toString();
    }

    public  String preview() {
        Table table = ds.getTable(tableName);
        return table.preview().toString();
    }

    private  String[] getFields(String dss,String tables) {
        List<Field> fields = table.getInfo().getFields();
        String[] fieldIds = new String[fields.size()];
        int index = 0;
        for (Field field : fields) {
            fieldIds[index++] = field.getName();
        }
        return fieldIds;
    }
    public  void ownBulkDelete(String where) {
        table.bulkDelete(where);
        commit();
    }
}
