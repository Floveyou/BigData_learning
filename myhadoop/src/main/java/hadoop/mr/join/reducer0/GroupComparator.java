package hadoop.mr.join.reducer0;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @user: share
 * @date: 2018/9/21
 * @description:
 */
public class GroupComparator extends WritableComparator {
    public GroupComparator() {
        super(CompKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompKey ck1 = (CompKey) a;
        CompKey ck2 = (CompKey) b;

        return ck1.getId().compareTo(ck2.getId());


    }
}
