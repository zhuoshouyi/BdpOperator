package com.topway.message;

/**
 * Created by xiaoyingfeng on 2017/11/27.
 *
 * 肖工通过http传输数据接应包
 */
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class SendMessage {
    protected static String testUrl = "http://172.31.150.13:8080/open/send";
    //    protected static String testUrl = "http://192.168.65.236:8080/open/send";
    protected static String appid = "04";
    protected static String secret = "tp3FisCjHuc8nkzxoKwe0ygieie";
    public static String SendMessage(String number,String content){
        TreeMap<String, String> apiparamsMap = new TreeMap<String, String>();
        String current_time=Util.getTime();
        apiparamsMap.put("format", "json");
        apiparamsMap.put("method", "send");
        apiparamsMap.put("appid",appid);
        apiparamsMap.put("version", "1.0");
        apiparamsMap.put("status", "0");
        apiparamsMap.put("access_token","");
        apiparamsMap.put("locale","");//需要获取的字段
        apiparamsMap.put("sign_method","MD5");
        apiparamsMap.put("timestamp",Util.getSystemTime("yyyyMMddHHmmssSSS"));
        apiparamsMap.put("content","{\"msgType\":\"1\",\"textInfo\":\""+content+"\",\"toList\":["+Util.Contacts(number)+"],\"channel\":\"01\",\"optusername\":\"changyg001\",\"corpid\":\"0\",\"busitype\":\"16\",\"issale\":\"0\",\"delayedtime\":\""+current_time+"\",\"servicenum\":\"5010\"}");
        System.out.println(apiparamsMap);
        String sign = Util.md5Signature(apiparamsMap,secret);
        apiparamsMap.put("sign", sign);
        StringBuilder param = new StringBuilder();
        for (Iterator<Map.Entry<String, String>> it = apiparamsMap.entrySet()
                .iterator(); it.hasNext();) {
            Map.Entry<String, String> e = it.next();
            param.append("&").append(e.getKey()).append("=").append(e.getValue());
        }
        return param.toString().substring(1);
    }
    public static void main(String []args) {
        try{
            System.out.println(args.length);
            if (args.length == 2){
                String num_str=args[0];
                String Message=args[1];
                String str = Util.postResult(testUrl, SendMessage(num_str,Message));
                System.out.println("result:"+str);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
