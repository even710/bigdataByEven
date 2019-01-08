package com.even.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
//import java.util.Properties;
import java.util.Set;

/**
 * Project Name: Web_App
 * Des:
 * Created by Even on 2018/12/4
 */
public class WordCount {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException, ParseException, ClassNotFoundException, IllegalAccessException, InstantiationException {
//        Properties properties = new Properties();
//        properties.load(WordCount.class.getClassLoader().getResourceAsStream("配置文件路徑"));
//        Path inpath = new Path(properties.getProperty("變量名"));
//        Class<?> mapper_class = Class.forName(properties.getProperty("mapper實現類路徑"));
//        Mapper mapper1 = (Mapper) mapper_class.newInstance();

        /*实例化Mapper*/
        Mapper mapper = new WorkCountMapper();
        Context context = new Context();
        /*1，构建hdfs客户端*/
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.11.136:9000/"), new Configuration(), "root");
        /*2，读取用户输入文件，已把文件放入hdfs的/even目录下*/
        RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(new Path("/even"), false);
        while (fileIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileIterator.next();
            FSDataInputStream in = fs.open(fileStatus.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));

            String line;
            while ((line = br.readLine()) != null) {
                //调用map方法执行业务逻辑
                mapper.map(line, context);
            }
            br.close();
            in.close();
        }
        /*4，建立输出目录*/
        Path out = new Path("/even/out");
        if (!fs.exists(out)) {
            fs.mkdirs(out);
        }
        /*5，获取缓存*/
        HashMap<Object, Object> contextMap = context.getContextMap();
        /*6，创建输出文件*/
        FSDataOutputStream out1 = fs.create(new Path("/even/out/out"));
        /*7，遍历map*/
        Set<Map.Entry<Object, Object>> entrySet = contextMap.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
            out1.write((entry.getKey().toString() + "\t" + entry.getValue().toString() + "\n").getBytes());
        }
        out1.close();
        fs.close();
        System.out.println("统计完毕！");
    }

//    public static String StringToDate(String dt) {
//
//        SimpleDateFormat sdf1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
//
//        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//        String date = "";
//        try {
//            date = sdf2.format(sdf1.parse(dt));
//        } catch (ParseException e) {
//            System.out.println(dt);
//            e.printStackTrace();
//        }
//        return date;
//    }


    //    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException, ParseException {
//        /*实例化Mapper*/
//        Mapper mapper = new WorkCountMapper();
//        Context context = new Context();
//        /*1，构建hdfs客户端*/
//        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.11.136:9000/"), new Configuration(), "root");
//        /*2，读取用户输入文件，已把文件放入hdfs的/even目录下*/
//        FSDataInputStream in = fs.open(new Path("/log/localhost.log.2018-12-03"));
//        BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
//        String line = null;
//        while ((line = br.readLine()) != null) {
//            //调用map方法执行业务逻辑
//            mapper.map(line, context);
//        }
//        br.close();
//        in.close();
//        /*4，建立输出目录*/
//        Path out = new Path("/log/out");
//        if (!fs.exists(out)) {
//            fs.mkdirs(out);
//        }
//        /*5，获取缓存*/
//        HashMap<Object, Object> contextMap = context.getContextMap();
//        /*6，创建输出文件*/
//        FSDataOutputStream out1 = fs.create(new Path("/log/out/close1203.log"));
//        FSDataOutputStream out2 = fs.create(new Path("/log/out/open1203.log"));
//        /*7，遍历map*/
//        Set<Map.Entry<Object, Object>> entrySet = contextMap.entrySet();
//        for (Map.Entry<Object, Object> entry : entrySet) {
//            String line1 = entry.getKey().toString();
////            if (line1.contains("SessionClosed")) {
////                if (line1.startsWith("Mon"))
////                    out1.write((line1.substring(0, 3) + "\t" + StringToDate(line1.substring(0, ("Mon Dec 03 00:00:07 CST 2018").length())) + "\tCST\t2018\t" + line1.substring(("Mon Dec 03 00:00:07 CST 2018").length()) + "\n").getBytes());
////                else {
////                    line1 = line1.substring(line1.lastIndexOf("-")+1).trim();
////                    out1.write((line1.substring(0, 3) + "\t" + StringToDate(line1.substring(0, ("Mon Dec 03 00:00:07 CST 2018").length())) + "\tCST\t2018\t" + line1.substring(("Mon Dec 03 00:00:07 CST 2018").length()) + "\n").getBytes());
////                }
////            }
////            if (line1.contains("SessionOpen")) {
////                if (line1.startsWith("Mon"))
////                    out2.write((line1.substring(0, 3) + "\t" + StringToDate(line1.substring(0, ("Mon Dec 03 00:00:07 CST 2018").length())) + "\tCST\t2018\t" + line1.substring(("Mon Dec 03 00:00:07 CST 2018").length()) + "\n").getBytes());
////                else {
////                    line1 = line1.substring(line1.lastIndexOf("-") + 1).trim();
////                    out2.write((line1.substring(0, 3) + "\t" + StringToDate(line1.substring(0, ("Mon Dec 03 00:00:07 CST 2018").length())) + "\tCST\t2018\t" + line1.substring(("Mon Dec 03 00:00:07 CST 2018").length()) + "\n").getBytes());
////                }
////            }
//        }
//        out1.close();
//        out2.close();
//        fs.close();
//        System.out.println("统计完毕！");
//    }

}
