package com.topway.message;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class Util {


    public static String createRandomSMID(){
        SimpleDateFormat formatter;
        formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        String ctime = formatter.format(new Date());
        String randata= createData.createData(6);
        String SMID =ctime+randata;
        return SMID;
    }


    public static String Contacts(String number ){
        String line = "";
        String[] lines = null;
        lines = number.split("\\|");
        System.out.println(lines.length);

        for(int i=0;i<lines.length;i++){

            line+="\""+lines[i]+"##"+ createRandomSMID() +"\"";

            if (i < lines.length - 1){

                line+=",";

            }
        }
        return line;
    }




    public static String  RandomD(){

        String randata= createData.createData(2);

        return randata;
    }


    public static String getSystemTime(String dateFormat) {
        String time = "";
        Date today = new Date();
        SimpleDateFormat format = new SimpleDateFormat(dateFormat);
        time = format.format(today);
        return time;
    }


    public static String getTime() {

        SimpleDateFormat formatter;
        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String ctime = formatter.format(new Date());
        return ctime;
    }



    public static String md5Signature(TreeMap<String, String> params,
                                      String secret) {
        String result = null;
        StringBuffer orgin = getBeforeSign(params, new StringBuffer(secret));
        if (orgin == null)
            return result;
        orgin.append(secret);
//		System.out.println(orgin.toString());
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            result = byte2hex(md.digest(orgin.toString().getBytes("utf-8")));
            System.out.println(result);
        } catch (Exception e) {
            throw new RuntimeException("sign error !");
        }
        return result;
    }

    /**
     */
    private static String byte2hex(byte[] b) {
        StringBuffer hs = new StringBuffer();
        String stmp = "";
        for (int n = 0; n < b.length; n++) {
            stmp = (Integer.toHexString(b[n] & 0XFF));
            if (stmp.length() == 1)
                hs.append("0").append(stmp);
            else
                hs.append(stmp);
        }
        return hs.toString().toUpperCase();
    }

    /**
     */
    private static StringBuffer getBeforeSign(TreeMap<String, String> params,
                                              StringBuffer orgin) {
        if (params == null)
            return null;
        Map<String, String> treeMap = new TreeMap<String, String>();
        treeMap.putAll(params);
        Iterator<String> iter = treeMap.keySet().iterator();
        while (iter.hasNext()) {
            String name = (String) iter.next();
            orgin.append(name).append(params.get(name));
        }
        return orgin;
    }

    public static String getResult(String urlStr, String content) {
        URL url = null;
        HttpURLConnection connection = null;
        System.out.println(urlStr+"?"+content);
        try {
            url = new URL(urlStr+"?"+content);
//			if("https".equalsIgnoreCase(url.getProtocol())){
//				try {
//					SslUtils.ignoreSsl();
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
            connection = (HttpURLConnection) url.openConnection();
            connection.connect();

            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    connection.getInputStream(), "utf-8"));
            StringBuffer buffer = new StringBuffer();
            String line = "";
            while ((line = reader.readLine()) != null) {
                buffer.append(line);
            }
            reader.close();
            return buffer.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        return null;
    }





    public static String postResult(String urlStr, String content) {
        URL url = null;
        HttpURLConnection connection = null;

        try {
            url = new URL(urlStr);

            connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("POST");
            connection.setUseCaches(false);
            connection.connect();

            DataOutputStream out = new DataOutputStream(connection
                    .getOutputStream());
            out.write(content.getBytes("utf-8"));
            out.flush();
            out.close();

            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    connection.getInputStream(), "utf-8"));
            StringBuffer buffer = new StringBuffer();
            String line = "";
            while ((line = reader.readLine()) != null) {
                buffer.append(line);
            }
            reader.close();
            return buffer.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        return null;
    }
}
