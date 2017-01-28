import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.KafkaSpoutTupleBuilder;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Created by ponteru07 on 2017/01/28.
 */
public class TopicsTupleBuilder<K, V> extends KafkaSpoutTupleBuilder<K, V> {
    public TopicsTupleBuilder(String... topics) {
        super(topics);
    }

    @Override
    public List<Object> buildTuple(ConsumerRecord<K, V> consumerRecord) {
        return new Values(consumerRecord.value());
    }
}
