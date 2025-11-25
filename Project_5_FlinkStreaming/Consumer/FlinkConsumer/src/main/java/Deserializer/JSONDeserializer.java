
public class JSONDeserializer implements DeserializationSchema<person> {


    private static final long serialVersionUID = 1L;
    private transient ObjectMapper mapper;
    @Override
    public void open(InitializationContext context) throws Exception {
        mapper = new ObjectMapper();
              mapper.registerModule(new JavaTimeModule());
    }

    @Override
    public person deserialize(byte[] message) throws IOException {
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

       
        return mapper.readValue(message, person.class);
    }


    @Override
    public boolean isEndOfStream(person nextElement) {
        return false;
    }

    
    @Override
    public TypeInformation<person> getProducedType() {
        return TypeInformation.of(person.class);
    }
}