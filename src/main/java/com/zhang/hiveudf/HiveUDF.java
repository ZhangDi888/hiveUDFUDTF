package com.zhang.hiveudf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class HiveUDF extends UDF {
    //自定义UDF,必须要实现这个方法
    public String evaluate(String line, String key) throws JSONException {
        //判断是否为空
        if (StringUtils.isBlank(line)) {
            return "";
        }
        //对文件以|的形式切割
        String[] split = line.split("\\|");

        //判断长度是否为2
        if (split.length != 2) {
            return "";
        }

        String serverTime = split[0];
        String baseJson = split[1];
        //解析json
        JSONObject base = new JSONObject(baseJson);
        //如果输入的st和key相同，返回serverTime
        if ("st".equals(key)) {
            return serverTime;
            //如果是et，先判断json里面有没有，有的话，返回et
        } else if ("et".equals(key)) {
            if (base.has("et")) {
                return base.getString("et");
            }
        } else {
            if (base.has("cm")) {
                JSONObject cm = base.getJSONObject("cm");
                if (cm.has(key)) {
                    cm.getString(key);
                }
            }
        }
        return "";
    }
}
