package Deserializer;

import Dto.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class myJsonDeserializier implements DeserializationSchema<Transaction>{
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper mapper;
    @Override
    public void open(InitializationContext context) throws Exception {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        if (mapper == null) {
            try {
                open(null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (message == null || message.length == 0) {
            return null;
        }


        return mapper.readValue(message, Transaction.class);
    }

    @Override
    public boolean isEndOfStream(Transaction transaction) {
        return false;
    }





    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}
