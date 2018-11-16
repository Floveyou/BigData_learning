import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class TestJson {

    @Test
    public void testJson() {

        // JSON 文本 {"name":"Tom","age":25}
        String text = "{\"name\":\"Tom\",\"age\":25}";

        // 将 JSON 转化为 JSONObject 格式
        JSONObject jo = JSON.parseObject(text);

        // 通过 Key 获取 Value
        Object name = jo.get("name");
        Object age = jo.get("age");
        System.out.println(name + ":" + age);
    }

    @Test
    public void testJsonArray() {
        // JSON 文本 {"person":[{"name":"Tomas","friends":["John","Jim"],"age":18},{"name":"Tom","friends":["Amy","Alice"],"age":28}]}
        String line = "{\"person\":[{\"name\":\"Tomas\",\"friends\":[\"John\",\"Jim\"],\"age\":18},{\"name\":\"Tom\",\"friends\":[\"Amy\",\"Alice\"],\"age\":28}]}";

        // 将 JSON 转化为 JSONObject 格式
        JSONObject jo = JSON.parseObject(line);

        // 通过 Key 获取 Value
        JSONArray jsonArray = jo.getJSONArray("person");

        for (Object object : jsonArray) {
            // 将 JSON 转化为 JSONObject 格式
            JSONObject jo2 = JSON.parseObject(object.toString());
            if (jo2.get("name").toString().equals("Tom")) {
                JSONArray jsonArray2 = jo2.getJSONArray("friends");
                for (Object object2 : jsonArray2) {
                    System.out.println(object2);
                }
            }
        }
    }

    @Test
    public void testTempTag() throws Exception {

        BufferedReader br = new BufferedReader(new FileReader("E:/file/temptags.txt"));

        String line = null;

        while ((line = br.readLine()) != null) {
            parseJson(line);
        }
    }

    // 解析 JSON 文本
    private void parseJson(String line) {

        String json = line.split("\t")[1];
        // 将 JSON 串转换成 JSONObject 格式
        JSONObject jo = JSON.parseObject(json);

        // 解析 extInfoList
        JSONArray extInfoList = jo.getJSONArray("extInfoList");
        // 非空判断和有效筛选
        if (extInfoList != null && extInfoList.size() != 0) {
            for (Object object : extInfoList) {
                //object => {"title":"contentTags","values":["午餐","分量适中"],"desc":"","defineType":0}
                JSONObject jo2 = (JSONObject) object;
                JSONArray extInfoList2 = jo2.getJSONArray("values");
                // 非空判断和有效筛选
                if (extInfoList2 != null && extInfoList2.size() != 0) {
                    for (Object object2 : extInfoList2) {
                        System.out.println(object2);
                    }
                }
            }
        }
    }
}
