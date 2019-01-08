package com.even.wordcount;

/**
 * Project Name: Web_App
 * Des:Mapper的实现
 * Created by Even on 2018/12/4
 */
public class WorkCountMapper implements Mapper {

    @Override
    public void map(String line, Context context) {
        /*1，切分数据，文件中的单词都是用兩個空格分开*/
        String[] words = line.split(" {2}");
        /*2，单词相同，value++*/
        for (String word : words) {
            Object value = context.get(word);
            if (null == value) {
                /*代表还没有计算该单词*/
                context.write(word, 1);
            } else {
                int v = (int) value;
                context.write(word, v + 1);
            }
        }
    }
}
