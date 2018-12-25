package hadoop.mr.userdraw;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 序列化与排序
 */
public class CompKey implements WritableComparable<CompKey> {
    private String appid;
    private int duration;

    //对频数进行倒排序
    public int compareTo(CompKey o) {

        if(this.duration == o.duration){
            return this.appid.compareTo(o.appid);
        }
        return o.duration -this.duration;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(appid);
        out.writeInt(duration);
    }

    public void readFields(DataInput in) throws IOException {
        appid = in.readUTF();
        duration = in.readInt();

    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public CompKey(String appid, int duration) {
        this.appid = appid;
        this.duration = duration;
    }

    public CompKey() {
    }

    @Override
    public String toString() {
        return appid + "\t" + duration;
    }
}
