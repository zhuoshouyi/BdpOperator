package com.main.java;

/**
 * Created by 肖英峰--Hansell on 2017/5/26.
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author 肖英峰
 * @function 根据oracle 视图中的表信息自动在hive ,BDP 中建立想对应的表
 */
public class CreateBDPTable {
    private static final String DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static Connection conn = null;
    /**
     * @param connStr
     * @return Connection
     */
    public static Connection getConnection(String connStr){
        try{
            Class.forName(DRIVER_CLASS);
            //System.out.println(connStr.split("|")[0]);
            conn=DriverManager.getConnection(connStr.split("\\|")[0],connStr.split("\\|")[1],connStr.split("\\|")[2]);
            //conn=DriverManager.getConnection("jdbc:oracle:thin:@172.31.150.10:1521:twboss2", "haizhi", "haizhi123");
            return conn;
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("2: "+e.getMessage());
        }
        return conn;
    }
    /***
     * 打印table structure
     * @throws SQLException
     * @return 返回table的表结构  以|分割； 例如TID|PRODUCTCODES|DEVICENOS
     */
    public static String sysoutTable(String connStr,String table)throws SQLException{

        getConnection(connStr);
        String columnNames="";
        try{
            Statement stmt = conn.createStatement();
            //System.out.println(conn);
            String sql="select * from "+table+" where rownum<1";
            //System.out.print(table+": ");
            ResultSet rs = stmt.executeQuery(sql);
            ResultSetMetaData rsmd=rs.getMetaData();
            for(int i=0;i<rsmd.getColumnCount();i++)
            {
                columnNames+=rsmd.getColumnName(i+1);
                columnNames+="|";
            }
            //System.out.println(columnNames);
            return columnNames;
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            conn.close();
        }
        return columnNames;
    }

    /**
     * @author xiaoyingfeng
     * @param connStr
     * @param ds ---BDP 中的数据源
     * @param tb ---BDP 中的表
     * @function 创建每天是全量的bdp 表： 第一个字段是rowid
     */
    public static void createBDPTabled(String connStr,String ds,String tb){
        try{
            String res=sysoutTable(connStr,tb);
            if(res==""||res.equals("")){
                System.out.println(" not found or others error");
            }else{
                DataOperator da = new  DataOperator();
                da.setDataSource(ds);
                //da.setTableName(tb);
                da.initDs();
                da.createTableHistory(tb,"ROWID|"+res);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public static void createBDPTableTwo(String connStr,String ds,String tb){
        try{
            String res=connStr;
            if(res==""||res.equals("")){
                System.out.println(" not found or others error");
            }else{
                DataOperator da = new  DataOperator();
                da.setDataSource(ds);
                //da.setTableName(tb);
                da.initDs();
                da.createTableHistory(tb,"ROWID|"+res);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    /**
     * @author xiaoyingfeng
     * @param connStr
     * @param ds
     * @param tb
     * @function 创建每天是增量的bdp 表
     */
    public static void createBDPTableIncre(String connStr,String ds,String tb){
        try{
            String res=sysoutTable(connStr,tb);
            if(res==""||res.equals("")){
                System.out.println(" not found or others error");
            }else{
                DataOperator da = new  DataOperator();
                da.setDataSource(ds);
                //da.setTableName(tb);
                da.initDs();
                da.createTableHistory(tb,res);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    /**
     * @author xiaoyingfeng
     * @param connStr
     * @param ds
     * @param tb
     * @function 创建每天是增量的bdp 表
     */
    public static void createBDPTableIncreSpecial(String connStr,String ds,String tb){
        try{
            String res=sysoutTable(connStr,tb);
            if(res==""||res.equals("")){
                System.out.println(" not found or others error");
            }else{
                DataOperator da = new  DataOperator();
                da.setDataSource(ds);
                //da.setTableName(tb);
                da.initDs();
                da.createTableHistorySpecial(tb,res);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * @author xiaoyingfeng
     * @param connStr
     * @param ds
     * @param tb
     * @function mscp库的表创建，根据提供的数据结构来创建表
     */
    public static void createMscp(String connStr,String ds,String tb){
        try{
            String res=connStr;
            if(res==""||res.equals("")){
                System.out.println(" not found or others error");
            }else{
                DataOperator da = new  DataOperator();
                da.setDataSource(ds);
               // da.setTableName(tb);
                da.initDs();
                da.createTableHistory(tb,res);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    /**
     * @author xiaoyingfeng
     * @param connStr
     * @param ds
     * @param tb
     * @function mscp库的表创建，根据提供的数据结构来创建表
     */
    public static void createQC(String connStr,String ds,String tb){
        try{
            String res=connStr;
            if(res==""||res.equals("")){
                System.out.println(" not found or others error");
            }else{
                DataOperator da = new  DataOperator();
                da.setDataSource(ds);
                // da.setTableName(tb);
                da.initDs();
                da.createTableQC(tb,res);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     *
     * @param connStr
     * @param ds
     * @param tb
     */
    public static void createNew(String connStr,String ds,String tb){
        try{
            String res=connStr;
            if(res==""||res.equals("")){
                System.out.println(" not found or others error");
            }else{
                DataOperator da = new  DataOperator();
                da.setDataSource(ds);
                // da.setTableName(tb);
                da.initDs();
                da.createTableNew(tb,res);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    /**
     * @author xiaoyingfeng
     * @param args
     * @description
     *  args[0] 是oracle 连接字符串    url|username|password
     *  args[1] ds
     *  args[2] table
     *  args[3] params  如果是incre,就是日增表的创建，如果是entire 则是日全表的创建
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        //System.out.println(args.length);
        if(args.length==4){
            if(args[0].contains("|")){
                if(args[3].equals("entire"))
                    createBDPTabled(args[0],args[1],args[2]);
                if(args[3].equals("entire_1"))
                    createBDPTableTwo(args[0],args[1],args[2]);
                if(args[3].equals("incre"))
                    createBDPTableIncre(args[0],args[1],args[2]);
                if(args[3].equals("special"))
                    createBDPTableIncreSpecial(args[0],args[1],args[2]);
                if(args[3].equals("mscp"))
                    createMscp(args[0],args[1],args[2]);
                if(args[3].equals("qc"))
                    createQC(args[0],args[1],args[2]);
                if(args[3].equals("new"))
                    createNew(args[0],args[1],args[2]);
               /* else{
                    System.out.println("Give Invalid parameters!opps,please check!");
                }*/
            }else{
                System.out.println("Invalid parameters!");
            }
        }else{
            System.out.println("iLLegaArgumentException happend,please check your parameters");
        }
    }

}
