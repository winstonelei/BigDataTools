package com.github.bigDataTools.util.serializable;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 *
 * @description <br>
 * @author winstone
 *
 */
public class KyroMessageDeserializer implements Deserializer<Object> {
    
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (data == null)
            return null;
        else
            return SerializeUtils.deserialize(data);
    }

    @Override
    public void close() {
        // nothing to do
    }
}
