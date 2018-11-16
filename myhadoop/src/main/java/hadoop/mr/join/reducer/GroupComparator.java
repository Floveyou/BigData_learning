package hadoop.mr.join.reducer;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组对比器，自定义 key 业务逻辑
 */
public class GroupComparator extends WritableComparator {

    public GroupComparator() {
        super(CompKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompKey ck1 = (CompKey) a;
        CompKey ck2 = (CompKey) b;

        return ck1.getId().compareTo(ck2.getId());
    }
}
