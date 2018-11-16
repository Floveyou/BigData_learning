package hadoop.mr.sort.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组对比器，自定义 key 业务逻辑，将 1902 20  1902 30 识别为同一个 key
 */
public class MyGroupComparator extends WritableComparator {

    // 必须写，创建实例必须写 true
    protected MyGroupComparator() {
        super(CompKey.class, true);
    }

    // 比较算法，只要 year 相等则证明 key 相等
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompKey ck1 = (CompKey) a;
        CompKey ck2 = (CompKey) b;

        return ck1.getYear().compareTo(ck2.getYear());
    }
}
