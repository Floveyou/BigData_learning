package hadoop.mr.temptags;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * 解析 JSON 文本工具类
 */
public class JsonUtil {

    // 解析 JSON 文本
    public static List<String> parseJson(String json) {

        List<String> list = new ArrayList<String>();

        // 将 JSON 串转换成 JSONObject 格式
        JSONObject jo = JSON.parseObject(json);

        // 解析 extInfoList
        JSONArray extInfoList = jo.getJSONArray("extInfoList");
        // 非空判断和有效筛选
        if (extInfoList != null && extInfoList.size() != 0) {
            for (Object object : extInfoList) {
                // object => {"title":"contentTags","values":["午餐","分量适中"],"desc":"","defineType":0}
                JSONObject jo2 = (JSONObject) object;

                // 筛选 title 为 contentTags 的value
                if (jo2.get("title").toString().equals("contentTags")) {
                    JSONArray extInfoList2 = jo2.getJSONArray("values");
                    // 非空判断和有效筛选
                    if (extInfoList2 != null && extInfoList2.size() != 0) {
                        for (Object object2 : extInfoList2) {
                            list.add(object2.toString());
                        }
                    }
                }
            }
        }
        return list;
    }
}
