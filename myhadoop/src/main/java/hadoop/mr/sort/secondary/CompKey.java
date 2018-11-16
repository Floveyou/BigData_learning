package hadoop.mr.sort.secondary;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 组合 Key , 包含自定义的排序规则 && 序列反序列化
 */
public class CompKey implements WritableComparable<CompKey> {

    private String year;
    private int temp;

    // 定义排序规则
    public int compareTo(CompKey o) {
        String oyear = o.getYear();
        String tyear = this.getYear();
        int otemp = o.getTemp();
        int ttemp = this.getTemp();

        // 如果传入的参数 year 和本身的 year 相同，则比较温度
        if (oyear.equals(tyear)) {
            return otemp - ttemp;
        }
        // 年份不同，则返回两个 year 的比较值
        return oyear.compareTo(tyear);
    }

    // 串行化
    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeInt(temp);
    }

    // 反串行化
    public void readFields(DataInput in) throws IOException {
        this.setYear(in.readUTF());
        this.setTemp(in.readInt());
    }

    @Override
    public String toString() {
        return "CompKey{" +
                "year='" + year + '\'' +
                ", temp=" + temp +
                '}';
    }

    public CompKey() {
    }

    public CompKey(String year, int temp) {
        this.year = year;
        this.temp = temp;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }
}
