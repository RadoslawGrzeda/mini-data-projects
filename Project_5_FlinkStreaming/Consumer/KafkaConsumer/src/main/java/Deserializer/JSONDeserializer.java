package Deserializer;

import Dto.person;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JSONDeserializer implements Deserializer<person>, DeserializationSchema<person> {
    private final ObjectMapper mapper = new ObjectMapper();


    @Override
    public person deserialize(byte[] bytes) throws IOException {
        if(bytes==null||bytes.length==0){
            return null;
        }
        try {
            return mapper.readValue(bytes, person.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEndOfStream(person person) {
        return false;
    }

    @Override
    public TypeInformation<person> getProducedType() {
        return TypeInformation.of(person.class);
    }

    @Override
    public person deserialize(String s, byte[] bytes) {
        return null;
    }
}
