package com.bloom.filter;

import com.main.java.DataOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import static com.main.java.ReadHdfsData.CsvDataEvDayOperator;
import static com.main.java.ReadHdfsData.MscpDataEvDayOperator;

/**
 * Created by hansell on 2018/1/23.
 */
public class PortalBloom {
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            if(args.length==6){
                //mscp 库中portal的数据导入 加入Bloom filter
                if(args[0].equals("bloom_filter"))
                    PortalDataBloomFilter(args[1],args[2],args[3],Integer.parseInt(args[4]),args[5]);
                //正常的portal 数据导入
                if(args[0].equals("mscp"))
                    MscpDataEvDayOperator(args[1],args[2],args[3],Integer.parseInt(args[4]),args[5]);
            }
            if(args[0].equals("csv"))
                CsvDataEvDayOperator(args[1],args[2],args[3],Integer.parseInt(args[4]),args[5],args[6]);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
     * @description portal 中SMID 和 TIME 的组合数据进行bloom filter 过滤
     */
    public static void PortalDataBloomFilter(String filePath,String ds,String tableName,int size,String initializeDate)
            throws IOException {
        System.out.println("Inserting data to"+ds+"."+tableName+"is begining,please don't leave me alone!");
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
        System.out.println(totalLen);
        if(totalLen>size){
            //bloom filter 声明
            BloomFilter bf = new BloomFilter(5, 1000000,3);
            long start = System.currentTimeMillis();
            try {
                FileSystem fs = FileSystem.get(URI.create(filePath),conf);
                fsr = fs.open(new Path(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(fsr));
                int k=0;
                String[][] datas = new String[size][];
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String[] data=lineTxt.split("\001");
                    /**if语句逻辑只针对portal
                       data[0] SMID  data[6] is TIME
                       rowkey 是SMID和TIME 精确到秒
                     */
                    String rowkey=data[0]+data[6].split("\\.")[0];
                    if(data[5].contains("NavCheck") && !bf.contains(rowkey)) {
                        bf.add(rowkey);
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
                        if (k == size) {
                            da.insertData(datas);
                            datas = new String[size][];
                            k = 0;
                        }
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
                System.out.println("Cost time is "+(end-start));

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
                    if(data[5].contains("NavCheck")) {
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
                }
                da.insertData(datas);
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
    }
    /**
     * @author xiaoyingfeng
     * @param filePath
     * @return
     * @throws IOException
     * @description 计算文件总记录条数，用于判断size
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
}
