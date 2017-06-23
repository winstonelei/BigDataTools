import com.github.bigDataTools.kafka.model.DefaultMessage;
import com.github.bigDataTools.kafka.producer.KafkaProducerManager;

/**
 * Created by winstone on 2017/6/12.
 */
public class TestKafkaProduce {

    public static void main(String[] args){

        KafkaProducerManager manager = KafkaProducerManager.getInstance();

        for(int i=0;i<=100;i++){
            manager.publish("demo-topic1", new DefaultMessage("fuck,man"));
            manager.publish("demo-topic2", new DefaultMessage("fuck,man"));

        }

    }


}
