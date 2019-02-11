package com.share.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 通过 UDTF 实现 Word Count
 */
public class MyUDTF extends GenericUDTF {

    // 表结构对象
    PrimitiveObjectInspector poi;

    // 定义了表结构（字段名称，字段类型）
    // argOIs => 输入字段的字段类型
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentException("参数个数只能为1");
        }

        // 如果输入字段类型非String，则抛异常
        ObjectInspector oi = argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector();

        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("参数非基本类型，需要基本类型String");
        }

        // 强转为基本类型对象检查器
        poi = (PrimitiveObjectInspector) oi;
        if (poi.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("参数非String，需要基本类型String");
        }

        // 构造字段名,word
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("word");

        // 构造字段类型,string
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        // 通过基本数据类型工厂获取 java基本类型 oi
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        //构造对象检查器
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    // 生成数据阶段
    @Override
    public void process(Object[] args) throws HiveException {
        // 得到一行数据
        String line = (String) poi.getPrimitiveJavaObject(args[0]);

        String[] arr = line.split(" ");

        for (String word : arr) {
            Object[] objects = new Object[1];
            objects[0] = word;
            forward(objects);
        }
    }

    // do nothing
    @Override
    public void close() throws HiveException {

    }
}
