package hadoop.mr.maxtemp0;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @user: share
 * @date: 2018/9/20
 * @description:
 */
public class CompKey implements WritableComparable {

    public int compareTo(Object o) {
        return 0;
    }

    public void write(DataOutput out) throws IOException {

    }

    public void readFields(DataInput in) throws IOException {

    }
}
