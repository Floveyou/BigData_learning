package hadoop.mr.join.reducer0;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @user: share
 * @date: 2018/9/21
 * @description:
 */
public class ReduceJoinReducer extends Reducer<CompKey, Text, Text, NullWritable> {

    @Override
    protected void reduce(CompKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> iter = values.iterator();

        //iter.hasNext();
        //用户行
        Text cusLine = iter.next();

        String[] cusArr = cusLine.toString().split(",");

        while (iter.hasNext()) {

            //order行
            String orderLine = iter.next().toString();

            //拼串操作，并将其输出

            String[] orderArr = orderLine.split(",");

            //uid + name + orderno + oprice
            String uid =  cusArr[0];
            String name = cusArr[1];
            String age = cusArr[2];

            String orderid = orderArr[0];
            String orderno = orderArr[1];
            String oprice = orderArr[2];


            String newLine = uid + "," + name + "," + age + "," + orderid + "," + orderno + "," + oprice;

            context.write(new Text(newLine) , NullWritable.get() );
        }


    }

}
