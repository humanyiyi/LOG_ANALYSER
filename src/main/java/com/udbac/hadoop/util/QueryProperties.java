package com.udbac.hadoop.util;


import java.io.*;
import java.util.Properties;

/**
 * Created by root on 2017/1/12.
 */

public class QueryProperties {

    public static String[] query()  {
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream("D:\\UDBAC\\LOG_ANALYSER\\src\\main\\resources\\application.properties"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
         String param = prop.getProperty("com.udbac.hadoop.util.query");
        String[] params = param.split(",");
//        for (int i = 0; i < params.length; i++){
//
//        }
//        System.out.println(params[1]);
        return params;
    }
    public static  void main(String[] args){
        query();
    }

}
