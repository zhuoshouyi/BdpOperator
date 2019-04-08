package com.bloom.filter;
import ie.ucd.murmur.MurmurHash;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.BitSet;
import java.util.Random;
/**
 * Created by hansell on 2018/1/19.
 * @function:bloom filter测试
 */
public class FastBloomFilter {
    public static void main(String [] args) {
        //声明bloom filter
        BloomFilter bf = new BloomFilter(5, 1000000,3);
        File file = new File("C:\\Users\\luobao\\IdeaProjects\\BdpOperator\\src\\com\\bloom\\filter\\data");
        StringBuilder result = new StringBuilder();
        try{
            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
            String s = null;
            while((s = br.readLine())!=null){//使用readLine方法，一次读一行
                //result.append(System.lineSeparator()+s);
                //System.out.println(s);
                System.out.println("Query for "+s+": "+"---" + bf.contains(s));
                System.out.println("Adding "+s);
                bf.add(s);
                System.out.println("Query for "+s+": " + bf.contains(s));
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
