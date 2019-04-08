package com.main.java;

/**
 * Created by 肖英峰--Hansell on 2017/5/25.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * @function 读取hdfs数据写入到BDP
 */
public class ReadHdfsData {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            System.out.println(args.length);
            if(args.length==6){
                //初始化 每天是增量数据
                if(args[0].equals("incre"))
                    IncreDataEvDayOperator(args[1],args[2],args[3],args[4],Integer.parseInt(args[5]));
                // 测试方法
                if(args[0].equals("test"))
                    Test(args[1],args[2],args[3],args[4]);
                //周全数据 写入
                if(args[0].equals("week_incre"))
                    IncreDataWeekOperator(args[1],args[2],args[3],args[4],Integer.parseInt(args[5]));
                //初始化 每天是全量数据的表 仅第一次初始化
                if(args[0].equals("entire"))
                    EntireDataEvDayOperator(args[1],Integer.parseInt(args[2]),args[3],args[4],args[5]);
                //mscp 库的数据导入
                if(args[0].equals("mscp"))
                    MscpDataEvDayOperator(args[1],args[2],args[3],Integer.parseInt(args[4]),args[5]);
                //工单调度数据写入
                if(args[0].equals("gddd"))
                    GdddDataEvDayOperator(args[1],args[2],args[3],Integer.parseInt(args[4]),args[5]);
            }
            if(args.length==7){
                //csv数据写入
                if(args[0].equals("csv"))
                    CsvDataEvDayOperator(args[1],args[2],args[3],Integer.parseInt(args[4]),args[5],args[6]);
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * @author xiaoyingfeng
     * @param date 数据写入bdp 时间
     * @param filePath 文件路径
     * @param ds    BDP中的数据源（文件夹）
     * @param tableName  BDP中的表
     * @param size      每次提交时的数据量大小
     * @throws IOException
     * @description 周全数据的导入
     */
    public static void IncreDataWeekOperator(String date,String filePath,String ds,String tableName,int size) throws IOException {

        System.out.println("Inserting data to "+ds+"."+tableName+" is begining,please don't leave me alone!");
        System.out.println(filePath);
        Configuration conf = new Configuration();
        StringBuffer buffer = new StringBuffer();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineTxt = null;
        SimpleDateFormat formatter;
        int totalLen=getLength(filePath);
        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DataOperator da= new DataOperator();
        da.setDataSource(ds);
        da.setTableName(tableName);
        da.init();
        // int insertCount = 0;  // insert的次数统计,十次insert触发commit
        int remainCount = 0;  // total/size得到的数值,然后每次自减,得到剩余insert次数。
        String startTime = formatter.format(new Date());
        System.out.println("start time is " + startTime);
        System.out.println(totalLen + " is total data.");
        if(totalLen>size){
            remainCount = totalLen / size;
            long start = System.currentTimeMillis();
            try {
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                int k=0;
                int len;  // 存放每一行数据的长度
                // 创建一个二维数组datas,用来存放size条数据
                String[][] datas = new String[size][];
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String[] data=lineTxt.split("\001");
                    len=data.length;
                    String ctime = formatter.format(new Date());


                    String[] dataT=new String[len+3];
                    for(int i=0;i<=len;i++){
                        if(i>len-3){
                            dataT[i]=data[i-1];
                        }else {
                            dataT[i] = data[i];
                        }
                    }

                    // dataT[len-2]是opt_ggs,如果为空,就是插入的数据;update是更新的数据;delete为删除的数据
                    // opt_ggs如果为空,将状态置为
                    if(dataT[len-2]=="\\N"||dataT[len-2].equals("\\N")){
                        dataT[len]="1900-01-01 00:00:00";
                        dataT[len-1]="Y";
                        dataT[len-2]="INSERT";
                    }

                    // 给opt_ggs不为空的行加入date,ctime和UUID
                    dataT[len-3]=date;
                    dataT[len+1]=ctime;
                    dataT[len+2]= UUID.randomUUID().toString();
                    datas[k]=dataT;
                    k++;
                    if(k==size){
                        da.insertData(datas);
                        datas=new String[size][];
                        k=0;
                        System.out.println("remain insert count:" + --remainCount);

                        // 如果insert计数到10次,则触发commit
//                        if (++insertCount == 10){
//                            insertCount = 0;
//                            System.out.println("剩余insert次数:" + --remainCount);
//                            da.commit();
//                        }
                    }
                }


                // total整除size后的余数,insert和commit
                if(k>0){
                    String[][] str2 = new String[k][];
                    for(int i=0;i<=k-1;i++){
                        str2[i]=datas[i];
                    }
                    da.insertData(str2);
                }
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));

                // 加commit
                start = System.currentTimeMillis();
                da.commit();
                end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));



            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        else{
            try{
                long start = System.currentTimeMillis();
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                String[][] datas = new String[totalLen][];
                int k=0;
                while ((lineTxt = bufferedReader.readLine()) != null){
                    String[] data=lineTxt.split("\001");
                    int len=data.length;
                    String ctime = formatter.format(new Date());
                    String[] dataT=new String[len+3];
                    for(int i=0;i<=len;i++){
                        if(i>len-3){
                            dataT[i]=data[i-1];
                        }else {
                            dataT[i] = data[i];
                        }
                    }
                    if(dataT[len-2]=="\\N"||dataT[len-2].equals("\\N")){
                        dataT[len]="1900-01-01 00:00:00";
                        dataT[len-1]="Y";
                        dataT[len-2]="INSERT";
                    }
                    dataT[len-3]=date;
                    dataT[len+1]=ctime;
                    dataT[len+2]= UUID.randomUUID().toString()+k;
                    datas[k]=dataT;
                    k++;
                }
                da.insertData(datas);
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));

                // 加commit
                start = System.currentTimeMillis();
                da.commit();
                end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));

            } catch (Exception e){
                e.printStackTrace();
            } finally{
                if (bufferedReader != null){
                    try{
                        bufferedReader.close();
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                }
            }
        }
        String endTime = formatter.format(new Date());
        System.out.println("end time is "+endTime);
    }

    /**
     * @author xiaoyingfeng
     * @param filePath
     * @return
     * @throws IOException
     * @description 计算文件总记录条数
     */
    public static int getLength(String filePath) throws IOException{
        Configuration conf = new Configuration();
        StringBuffer buffer = new StringBuffer();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        int length=0;
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            fsr = fs.open(new Path(filePath));
            bufferedReader = new BufferedReader(new InputStreamReader(fsr));
            length=(int)(bufferedReader.lines().count());
        }
        catch (Exception e){
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return length;
    }

    /**
     * @author hansell
     * @param filePath  文件路径
     * @param ds     BDP 中的数据源
     * @param tb     BDP中的表
     * @param size   一次写入数据量大小
     * @param initializeDate   初始化时间
     * @param delimiter   字段分隔符  csv中一般有两种   "\001" ","
     */
    public static void CsvDataEvDayOperator(String filePath,String ds,String tb,int size,
                                            String initializeDate,String delimiter){
        System.out.println("Inserting data to "+ds+"."+tb+" is begining,please don't leave me alone!");
        System.out.println(filePath);
        Configuration conf = new Configuration();
        StringBuffer buffer = new StringBuffer();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineTxt = null;
        SimpleDateFormat formatter;
        int totalLen=135185;
        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //连接bdp客户端
        DataOperator da= new DataOperator();
        da.setDataSource(ds);
        da.setTableName(tb);
        da.init();
        int remainCount = 0;  // total/size得到的数值,然后每次自减,得到剩余insert次数。
        System.out.println(totalLen + " is total data.");
        if(totalLen>size){
            remainCount = (int) Math.ceil(totalLen / size);
            long start = System.currentTimeMillis();
            try {
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                // k  插入的条数
                int k=0;
                String[][] datas = new String[size][];
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String[] data=lineTxt.split(delimiter);
                    int len=data.length;
                    String ctime = formatter.format(new Date());
                    String[] dataT=new String[len+5];
                    for(int i=0;i<=len-1;i++){
                        dataT[i]=data[i].replace("\"","");
                    }
                    dataT[len+2]=initializeDate+" 00:00:00";
                    dataT[len+1]="Y";
                    dataT[len]="INSERT";
                    dataT[len+3]=ctime;
                    dataT[len+4]= UUID.randomUUID().toString();
                    datas[k]=dataT;
                    k++;
                    if(k==size){
                        da.insertData(datas);
                        //DataOperator.commit(ds,tb);
                        datas=new String[size][];
                        k=0;
                        System.out.println("remain insert count:" + --remainCount);
                    }
                }
                if(k>0){
                    String[][] str2 = new String[k][];
                    for(int i=0;i<=k-1;i++){
                        str2[i]=datas[i];
                    }
                    da.insertData(str2);
                }
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));


                // 加commit
                start = System.currentTimeMillis();
                da.commit();
                end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        else{
            try{
                long start = System.currentTimeMillis();
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                String[][] datas = new String[totalLen][];
                int k=0;
                while ((lineTxt = bufferedReader.readLine()) != null){
                    String[] data=lineTxt.split(",");
                    int len=data.length;
                    String ctime = formatter.format(new Date());
                    String[] dataT=new String[len+5];
                    for(int i=0;i<=len-1;i++){
                        dataT[i]=data[i];
                    }
                    dataT[len+2]=initializeDate+" 00:00:00";
                    dataT[len+1]="Y";
                    dataT[len]="INSERT";
                    dataT[len+3]=ctime;
                    dataT[len+4]= UUID.randomUUID().toString();
                    datas[k]=dataT;
                    k++;
                }
                //DataOperator.insertData(datas,ds,tb);
                //DataOperator.commit(ds,tb);
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));


                // 加commit
                start = System.currentTimeMillis();
                da.commit();
                end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));

            } catch (Exception e){
                e.printStackTrace();
            } finally{
                if (bufferedReader != null){
                    try{
                        bufferedReader.close();
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                }
            }
        }
    }
    /**
     * @author xiaoyingfeng
     * @param filePath  文件路径
     * @description csv files
     *
     */
    public static void CsvTest(String filePath){
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        //conf.set("mapred.child.java.opts","-Xmx8000m");
        StringBuffer buffer = new StringBuffer();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineTxt = null;
        try{
            long start = System.currentTimeMillis();
            FileSystem fs = FileSystem.get(URI.create(filePath),conf);
            fsr = fs.open(new Path(filePath));
            bufferedReader = new BufferedReader(new InputStreamReader(fsr));
            while ((lineTxt = bufferedReader.readLine()) != null) {
                //String[] data = lineTxt.split("\001");
                System.out.println(lineTxt);
            }
            long end = System.currentTimeMillis();
            System.out.println("Cost time is "+(end-start));
        } catch (Exception e){
            e.printStackTrace();
        } finally{
            if (bufferedReader != null){
                try{
                    bufferedReader.close();
                } catch (IOException e){
                    e.printStackTrace();
                }
            }
        }

}
    /**
     * @author xiaoyingfeng
     * @param filePath  文件路径
     * @param initialize Y-第一次全量初始化 需要给optime_ggs valid_ggs opt_ggs 这三个参数加入默认值
     *                   N-每天增量插入数据 则不需要更新这三个字段
     * @param ds   插入到bdp表的数据源
     * @param tableName  bdp表名
     * @param size  每次提交数据量 数据字段较多建议100000-150000 数据字段较少建议200000-250000
     * @throws IOException
     * @description 第一次全量数据初始化，每天增量更新数据
     */
    public static void IncreDataEvDayOperator(String filePath,String initialize,String ds,String tableName,int size) throws IOException {

        System.out.println("Inserting data to "+ds+"."+tableName+" is begining,please don't leave me alone!");
        System.out.println(filePath);
        Configuration conf = new Configuration();
        //conf.set("mapred.child.java.opts","-Xmx8000m");
        StringBuffer buffer = new StringBuffer();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineTxt = null;
        SimpleDateFormat formatter;
        int totalLen=getLength(filePath);
        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //连接bdp客户端
        DataOperator da= new DataOperator();
        da.setDataSource(ds);
        da.setTableName(tableName);
        da.init();
        int remainCount = 0;  // total/size得到的数值,然后每次自减,得到剩余insert次数。
        String startTime = formatter.format(new Date());
        System.out.println("start time is "+startTime);
        System.out.println(totalLen + " is total data.");
        if(totalLen>size){
            remainCount = (int) Math.ceil(totalLen / size);
            long start = System.currentTimeMillis();
            try {
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                int k=0; // k  插入的条数
                String[][] datas = new String[size][];
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String[] data=lineTxt.split("\001");
                    int len=data.length;
                    String ctime = formatter.format(new Date());
                    String[] dataT=new String[len+2];
                    for(int i=0;i<=len-1;i++){
                        dataT[i]=data[i];
                    }
                    if(initialize=="Y"||initialize.equals("Y")){
                        dataT[len-1]="1900-01-01 00:00:00";
                        dataT[len-2]="Y";
                        dataT[len-3]="INSERT";
                    }
                    dataT[len]=ctime;
                    dataT[len+1]= UUID.randomUUID().toString();
                    datas[k]=dataT;
                    k++;
                    if(k==size){
                        da.insertData(datas);
                        datas=new String[size][];
                        k=0;
                        System.out.println("remain insert count:" + --remainCount);
                    }
                }
                if(k>0){
                    String[][] str2 = new String[k][];
                    for(int i=0;i<=k-1;i++){
                        str2[i]=datas[i];
                    }
                    da.insertData(str2);
                }
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));


                // 加commit
                start = System.currentTimeMillis();
                da.commit();
                end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));


            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        else{
            try{
                long start = System.currentTimeMillis();
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                String[][] datas = new String[totalLen][];
                int k=0;
                while ((lineTxt = bufferedReader.readLine()) != null){
                    String[] data=lineTxt.split("\001");
                    int len=data.length;
                    String ctime = formatter.format(new Date());
                    String[] dataT=new String[len+2];
                    for(int i=0;i<=len-1;i++){
                        dataT[i]=data[i];
                    }
                    if(initialize=="Y"||initialize.equals("Y")){
                        dataT[len-1]="1900-01-01 00:00:00";
                        dataT[len-2]="Y";
                        dataT[len-3]="INSERT";
                    }
                    dataT[len]=ctime;
                    dataT[len+1]= UUID.randomUUID().toString()+k;
                    datas[k]=dataT;
                    k++;
                }
         /*  for(int i=0;i<=datas.length-1;i++) {
                System.out.println();
               System.out.print(datas[i]);
                for (int j = 0; j <= datas[i].length-1; j++) {
                    System.out.print(datas[i][j] + ",");
                }
            }*/
                da.insertData(datas);
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));


                // 加commit
                start = System.currentTimeMillis();
                da.commit();
                end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));

            } catch (Exception e){
                e.printStackTrace();
            } finally{
                if (bufferedReader != null){
                    try{
                        bufferedReader.close();
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                }
            }
        }
        String endTime = formatter.format(new Date());
        System.out.println("end time is "+endTime);
    }

    /**
     * @author xiaoyingfeng
     * @param targetPath
     * @param data
     * @description 写入数据
     */

    public static void loadData2hdfs(String targetPath,String data){
        //FileInputStream in = null;
        //FSDataOutputStream out = null;
        OutputStream out=null;
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(targetPath), conf);
            Path path = new Path(targetPath);
            //boolean result = fs.isDirectory(path);
            //logger.info("是否为文件夹："+result);
             out = fs.append(path);   //创建文件
            //两个方法都用于文件写入，好像一般多使用后者
            Writer writer = new OutputStreamWriter(out);
            BufferedWriter bfWriter = new BufferedWriter(writer);
            bfWriter.write(data);
            //out.write(words.getBytes("UTF-8"));
            if(null != bfWriter){
                bfWriter.close();
            }
            if(null != writer){
                writer.close();
            }
            System.out.println("success!!!");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * gddd
     * @author xiaoyingfeng
     * @param filePath  文件路径
     * @param initializeDate 给出的数据产生时间
     * @param ds   插入到bdp表的数据源
     * @param tableName  bdp表名
     * @param size  每次提交数据量 数据字段较多建议100000-150000 数据字段较少建议200000-250000
     * @throws IOException
     * @description mscp 库的数据写入 因为mscp 比较特殊 是从文件到hdfs 所有每天的数据都是新增
     */
    public static void GdddDataEvDayOperator(String filePath,String ds,String tableName,int size,String initializeDate) throws IOException {

        System.out.println("Inserting data to "+ds+"."+tableName+" is begining,please don't leave me alone!");
        System.out.println(filePath);
        Configuration conf = new Configuration();
        StringBuffer buffer = new StringBuffer();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineTxt = null;
        SimpleDateFormat formatter;
        int totalLen=getLength(filePath);
        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //连接bdp客户端
        DataOperator da= new DataOperator();
        da.setDataSource(ds);
        da.setTableName(tableName);
        da.init();
        int remainCount = 0;  // total/size得到的数值,然后每次自减,得到剩余insert次数。
        System.out.println(totalLen + " is total data.");
        if(totalLen>size){
            remainCount = (int) Math.ceil(totalLen / size);
            long start = System.currentTimeMillis();
            try {
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                int k=0;
                String[][] datas = new String[size][];
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String[] data=lineTxt.split("\t");
                    int len=data.length;
                    String ctime = formatter.format(new Date());
                    String[] dataT=new String[len+5];
                    for(int i=0;i<=len-1;i++){
                        dataT[i]=data[i];
                    }
                    dataT[len+2]=initializeDate+" 00:00:00";
                    dataT[len+1]="Y";
                    dataT[len]="INSERT";
                    dataT[len+3]=ctime;
                    dataT[len+4]= UUID.randomUUID().toString();
                    datas[k]=dataT;
                    k++;
                    if(k==size){
                        da.insertData(datas);
                        datas=new String[size][];
                        k=0;
                        System.out.println("remain insert count:" + --remainCount);
                    }
                }
                if(k>0){
                    String[][] str2 = new String[k][];
                    for(int i=0;i<=k-1;i++){
                        str2[i]=datas[i];
                    }
                  /*  DataOperator dor= new DataOperator();
                    dor.setDataSource(ds);
                    dor.setTableName(tableName);
                    dor.init();*/
                    da.insertData(str2);
                }
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));


                // 加commit
                start = System.currentTimeMillis();
                da.commit();
                end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));


            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        else{
            try{
                long start = System.currentTimeMillis();
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                String[][] datas = new String[totalLen][];
                int k=0;
                while ((lineTxt = bufferedReader.readLine()) != null){
                    String[] data=lineTxt.split("\t");
                    int len=data.length;
                    String ctime = formatter.format(new Date());
                    String[] dataT=new String[len+5];
                    for(int i=0;i<=len-1;i++){
                        dataT[i]=data[i];
                    }
                    dataT[len+2]=initializeDate+" 00:00:00";
                    dataT[len+1]="Y";
                    dataT[len]="INSERT";
                    dataT[len+3]=ctime;
                    dataT[len+4]= UUID.randomUUID().toString();
                    datas[k]=dataT;
                    k++;
                }
                da.insertData(datas);
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));


                // 加commit
                start = System.currentTimeMillis();
                da.commit();
                end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));

            } catch (Exception e){
                e.printStackTrace();
            } finally{
                if (bufferedReader != null){
                    try{
                        bufferedReader.close();
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                }
            }
        }
    }
    /**
     * @author xiaoyingfeng
     * @param filePath  文件路径
     * @param initializeDate 给出的数据产生时间
     * @param ds   插入到bdp表的数据源
     * @param tableName  bdp表名
     * @param size  每次提交数据量 数据字段较多建议100000-150000 数据字段较少建议200000-250000
     * @throws IOException
     * @description mscp 库的数据写入 因为mscp 比较特殊 是从文件到hdfs 所有每天的数据都是新增
     */
    public static void MscpDataEvDayOperator(String filePath,String ds,String tableName,int size,String initializeDate) throws IOException {

        System.out.println("Inserting data to "+ds+"."+tableName+" is begining,please don't leave me alone!");
        System.out.println(filePath);
        Configuration conf = new Configuration();
        StringBuffer buffer = new StringBuffer();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineTxt = null;
        SimpleDateFormat formatter;
        int totalLen=getLength(filePath);
        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //连接bdp客户端
        DataOperator da= new DataOperator();
        da.setDataSource(ds);
        da.setTableName(tableName);
        da.init();
        int remainCount = 0;  // total/size得到的数值,然后每次自减,得到剩余insert次数。
        System.out.println(totalLen + " is total data.");
        if(totalLen>size){
            remainCount = (int) Math.ceil(totalLen / size);
            long start = System.currentTimeMillis();
            try {
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                // k  插入的条数
                int k=0;
                String[][] datas = new String[size][];
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String[] data=lineTxt.split("\001");
                    int len = data.length;
                    String ctime = formatter.format(new Date());
                    String[] dataT = new String[len + 5];
                    for (int i = 0; i <= len - 1; i++) {
                        dataT[i] = data[i];
                    }
                    dataT[len + 2] = initializeDate + " 00:00:00";
                    dataT[len + 1] = "Y";
                    dataT[len] = "INSERT";
                    dataT[len + 3] = ctime;
                    dataT[len + 4] = UUID.randomUUID().toString();
                    datas[k] = dataT;
                    k++;

                    // 对size条数据进行加工完毕判断
                    if (k == size) {
                        da.insertData(datas);
                        datas = new String[size][];
                        k = 0;
                        System.out.println("remain insert count:" + --remainCount);
                    }
                }

                if(k>0){
                    String[][] str2 = new String[k][];
                    for(int i=0;i<=k-1;i++){
                        str2[i]=datas[i];
                    }
                  /*  DataOperator dor= new DataOperator();
                    dor.setDataSource(ds);
                    dor.setTableName(tableName);
                    dor.init();*/
                    da.insertData(str2);
                }
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));


                // 加commit
                start = System.currentTimeMillis();
                da.commit();
                end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));


            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        else{
            try{
                long start = System.currentTimeMillis();
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                String[][] datas = new String[totalLen][];
                int k=0;
                while ((lineTxt = bufferedReader.readLine()) != null){
                    String[] data=lineTxt.split("\001");
                    int len = data.length;
                    String ctime = formatter.format(new Date());
                    String[] dataT = new String[len + 5];
                    for (int i = 0; i <= len - 1; i++) {
                        dataT[i] = data[i];
                    }
                    dataT[len + 2] = initializeDate + " 00:00:00";
                    dataT[len + 1] = "Y";
                    dataT[len] = "INSERT";
                    dataT[len + 3] = ctime;
                    dataT[len + 4] = UUID.randomUUID().toString();
                    datas[k] = dataT;
                    k++;

                }
                da.insertData(datas);
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));
                // System.out.println("Insert cost time is "+(end-start));

                // 加commit
                start = System.currentTimeMillis();
                da.commit();
                end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));
                // System.out.println("Commit cost time is "+(end-start));

            } catch (Exception e){
                e.printStackTrace();
            } finally{
                if (bufferedReader != null){
                    try{
                        bufferedReader.close();
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                }
            }
        }


    }

    /**
     * @author xiaoyingfeng
     * @param filePath
     * @param initialize
     * @param ds
     * @param tableName
     * @throws IOException
     * @description test method 只往表中插入三条数据
     */
    public static void Test(String filePath,String initialize,String ds,String tableName) throws IOException {

        System.out.println(filePath);
        Configuration conf = new Configuration();
        //conf.set("mapred.child.java.opts","-Xmx8000m");
        StringBuffer buffer = new StringBuffer();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineTxt = null;
        SimpleDateFormat formatter;
        int totalLen=getLength(filePath);
        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        try{
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                int k=0;
                String[][] datas = new String[3][];
                while ((lineTxt = bufferedReader.readLine()) != null)
                {
                    String[] data=lineTxt.split("\001");
                    int len=data.length;
                    System.out.println(len+"this is one data length");
                    String ctime = formatter.format(new Date());
                    //会多个rowid 而bdp 表中没有rowid 字段
                    for(int i=0;i<=data.length-1;i++) {
                       System.out.println(data[i]);
                    }
                    //System.out.println(data.length+"----");
                    String[] dataT=new String[len+2];
                    for(int i=0;i<=len-1;i++){
                        dataT[i]=data[i];
                    }
                    if(initialize=="Y"||initialize.equals("Y")){
                        dataT[len-1]="1900-01-01"; //optime_ggs
                        dataT[len-2]="Y";  //valid_ggs
                        dataT[len-3]="insert"; //opt_ggs
                    }
                    dataT[len]=ctime.toString();
                    dataT[len+1]= UUID.randomUUID().toString();
                    datas[k]=dataT;
                    k++;
                    if(k==3)
                        break;
                }
                System.out.println(datas.length);
                System.out.println(datas[0].length);
                for(int i=0;i<=datas.length-1;i++) {
                  System.out.println();
               //System.out.print(datas[i]);
                for (int j = 0; j <= datas[i].length-1; j++) {
                    System.out.print(datas[i][j] + ",");
                }
            }
                //DataOperator.insertTest(datas,ds,tableName);
             //   DataOperator.commit(ds,tableName);
                //System.out.println(list.size());
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));



        } catch (Exception e)
            {
                e.printStackTrace();
            } finally
            {
                if (bufferedReader != null)
                {
                    try
                    {
                        bufferedReader.close();
                    } catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }
    /**
     * @author xiaoyingfeng
     * @param filePath
     * @param initializeSize
     * @param ds
     * @param tableName
     * @throws IOException
     * @description 每天全量数据表的初始化导入,表中增加有rowid字段 没有 optime_ggs valid_ggs opt_ggs 这三个字段
     * 需要在每条数据中加5个字段
     */
    public static void EntireDataEvDayOperator(String filePath,int initializeSize,String ds,String tableName,String dt) throws IOException {
        System.out.println("Inserting data to "+ds+"."+tableName+" is begining,please don't leave me alone!");
        System.out.println(filePath);
        Configuration conf = new Configuration();
        //conf.set("mapred.child.java.opts","-Xmx8000m");
        StringBuffer buffer = new StringBuffer();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineTxt = null;
        SimpleDateFormat formatter;
        int totalLen=getLength(filePath);
        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //连接bdp客户端
        DataOperator da= new DataOperator();
        da.setDataSource(ds);
        da.setTableName(tableName);
        da.init();
        String startTime = formatter.format(new Date());
        System.out.println("start time is "+startTime);
        int remainCount = 0;  // total/size得到的数值,然后每次自减,得到剩余insert次数。
        System.out.println(totalLen + " is total data.");
        if(totalLen>initializeSize){
            remainCount = (int) Math.ceil(totalLen / initializeSize);
            long start = System.currentTimeMillis();
            try
            {
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                int k=0;
                String[][] datas = new String[initializeSize][];
                while ((lineTxt = bufferedReader.readLine()) != null)
                {
                    String[] data=lineTxt.split("\001");
                    int len=data.length;
                    String ctime = formatter.format(new Date());
                    String[] dataT=new String[len+5];
                    for(int i=0;i<=len-1;i++){
                        dataT[i]=data[i];
                    }
                    dataT[len+2]=dt+" 00:00:00";
                    dataT[len+1]="Y";
                    dataT[len]="INSERT";
                    dataT[len+3]=ctime;
                    dataT[len+4]= UUID.randomUUID().toString();
                    datas[k]=dataT;
                    k++;
                    if(k==initializeSize){
                        da.insertData(datas);
                        datas=new String[initializeSize][];
                        k=0;
                        System.out.println("remain insert count:" + --remainCount);
                    }
                }
                if(k>0){
                    String[][] str2 = new String[k][];
                    for(int i=0;i<=k-1;i++){
                        str2[i]=datas[i];
                    }
                 da.insertData(str2);
                }
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));


                // 加commit
                start = System.currentTimeMillis();
                da.commit();
                end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));

            } catch (Exception e){
                e.printStackTrace();
            } finally{
                if (bufferedReader != null){
                    try{
                        bufferedReader.close();
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                }
            }
        }
        else{
            try{
                long start = System.currentTimeMillis();
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                String[][] datas = new String[totalLen][];
                int k=0;
                while ((lineTxt = bufferedReader.readLine()) != null){
                    String[] data=lineTxt.split("\001");
                    int len=data.length;
                    String ctime = formatter.format(new Date());
                    String[] dataT=new String[len+5];
                    for(int i=0;i<=len-1;i++){
                        dataT[i]=data[i];
                    }
                    dataT[len+2]=dt+" 00:00:00";
                    dataT[len+1]="Y";
                    dataT[len]="INSERT";
                    dataT[len+3]=ctime;
                    dataT[len+4]= UUID.randomUUID().toString();
                    datas[k]=dataT;
                    k++;
                }
                da.insertData(datas);
                long end = System.currentTimeMillis();
                System.out.println("Insert Cost time is "+(end-start));


                // 加commit
                start = System.currentTimeMillis();
                da.commit();
                end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));

            } catch (Exception e){
                e.printStackTrace();
            } finally{
                if (bufferedReader != null){
                    try{
                        bufferedReader.close();
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                }
            }
        }
        String endTime = formatter.format(new Date());
        System.out.println("end time is "+endTime);
    }
}
