package hadoop.mr.temptags;

/**
 * 组合 Key , 包含自定义的排序规则
 */
public class CompKey implements Comparable<CompKey> {

    private String tag;
    private int sum;

    public int compareTo(CompKey o) {

        if (this.sum == o.sum) {
            return this.tag.compareTo(o.tag);
        }
        return o.sum - this.sum;

    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public CompKey(String tag, int sum) {
        this.tag = tag;
        this.sum = sum;
    }

    @Override
    public String toString() {
        return tag + "_" + sum;
    }
}