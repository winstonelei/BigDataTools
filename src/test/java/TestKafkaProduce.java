import com.github.bigDataTools.kafka.model.DefaultMessage;
import com.github.bigDataTools.kafka.producer.KafkaProducerManager;
import org.junit.Test;

/**
 * Created by winstone on 2017/6/12.
 */
public class TestKafkaProduce {


    @Test
    public void testSendMessage(){
        KafkaProducerManager manager = KafkaProducerManager.getInstance();
        for(int i=0;i<=100;i++){
            manager.publish("order", new DefaultMessage("fuck,man"));
        }
    }

}
