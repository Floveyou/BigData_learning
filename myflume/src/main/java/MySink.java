import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.sink.AbstractSink;

import java.util.Map;

/**
 * @user: share
 * @date: 2019/2/17
 * @description:
 */
public class MySink extends AbstractSink {


    public Status process() throws EventDeliveryException {
        //  Status.READY 说明channel中有事件，可以继续处理
        //  Status.Backoff 说明channel中没有事件，无需继续处理
        Status result = Status.READY;
        Channel channel = getChannel();
        // transaction事务
        // begin() 事务开始，前提是上个事务需要关闭
        //commit() 事务处理完成，即可提交
        //rollback() 事务出现异常，即回滚，就是将出现异常的event重新放回channel
        //close() 事务完成之后，需要close
        Transaction transaction = channel.getTransaction();
        Event event = null;

        try {
            transaction.begin();
            //从通道中获取数据
            event = channel.take();

            if (event != null) {
                StringBuffer sb = new StringBuffer();
                Map<String, String> map = event.getHeaders();

                for (Map.Entry<String, String> entry : map.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    sb.append(key + ":" + value + ",");
                }
                String body = new String(event.getBody());
                String headers = sb.toString();
                String newStr = "";
                if (headers.length() != 0) {
                    newStr = headers.substring(0, headers.length() - 1);
                }
                System.out.println("头部=" + newStr + "\t" + "身体=" + body);

            } else {
                // No event found, request back-off semantics from the sink runner
                result = Status.BACKOFF;
            }
            transaction.commit();
        } catch (Exception ex) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to log event: " + event, ex);
        } finally {
            transaction.close();
        }

        return result;
    }
}
