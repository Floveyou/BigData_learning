package hadoop.mr.top10;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 组合 Key , 包含自定义的排序规则 && 序列反序列化
 */
public class CompKey implements WritableComparable<CompKey> {

    private String pwd;
    private int sum;

    // 对频数进行倒排序
    public int compareTo(CompKey o) {

        if (this.sum == o.sum) {
            return this.pwd.compareTo(o.pwd);
        }
        return o.sum - this.sum;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(pwd);
        out.writeInt(sum);
    }

    public void readFields(DataInput in) throws IOException {
        pwd = in.readUTF();
        sum = in.readInt();

    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public CompKey() {
    }

    public CompKey(String pwd, int sum) {
        this.pwd = pwd;
        this.sum = sum;
    }

    @Override
    public String toString() {
        return pwd + "\t" + sum;
    }
}
