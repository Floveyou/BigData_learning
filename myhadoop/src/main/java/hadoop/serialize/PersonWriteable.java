package hadoop.serialize;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义 PersonWriteable 实现 Person 的序列化与反序列化
 */
public class PersonWriteable implements Writable {
    // 定义 person
    private Person person;

    // 设置 get 方法
    public Person getPerson() {
        return person;
    }

    // 设置 set 方法
    public void setPerson(Person person) {
        this.person = person;
    }

    /**
     * 重写序列化方法
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        // 序列化 name 字段
        out.writeUTF(person.getName());
        // 序列化 age 字段
        out.writeInt(person.getAge());
    }

    /**
     * 重写反序列化方法
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        // 初始化 person
        person = new Person();
        // 反序列化 name 字段
        person.setName(in.readUTF());
        // 反序列化 age 字段
        person.setAge(in.readInt());
    }
}
