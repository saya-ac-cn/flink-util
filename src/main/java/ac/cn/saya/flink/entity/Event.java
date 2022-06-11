package ac.cn.saya.flink.entity;
import java.sql.Timestamp;
/**
 * @author saya
 * @title: Event
 * @projectName flink-util
 * @description: TODO
 * @date: 2022/6/11 17:12
 * @description:
 */

public class Event {
   public String user;
   public String url;
   public Long timestamp;
   public Event() {
   }
   public Event(String user, String url, Long timestamp) {
      this.user = user;
      this.url = url;
      this.timestamp = timestamp;
   }
   @Override
   public String toString() {
      return "Event{" +
              "user='" + user + '\'' +
              ", url='" + url + '\'' +
              ", timestamp=" + new Timestamp(timestamp) +
              '}';
   }
}
