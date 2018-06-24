package com.cn.test.hive;

import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.Assert;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class TestUDF extends UDF {
   /* create temporary function my_lower as 'com.example.hive.udf.Lower';
    hive> add jar my_jar.jar;
    Added my_jar.jar to class path*/

    public Text evaluate(final Text s) {
        if (s == null) { return null; }

        StringBuilder ret = new StringBuilder();

        String[] items = s.toString().split("\\.");
        if (items.length != 4)
            return null;

        for (String item : items) {
            StringBuilder sb = new StringBuilder();
            int a = Integer.parseInt(item);
            for (int i=0; i<8; i++) {
                sb.insert(0, a%2);
                a = a/2;
            }
            ret.append(sb);
        }

        return new Text(ret.toString());
    }

    public static void main(String[] args) {
        String ip = "112.117.138.216";
        TestUDF unmasker = new TestUDF();
        System.out.println(unmasker.evaluate(new Text(ip)));
    }
}