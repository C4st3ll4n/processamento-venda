package deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.Venda;
import org.apache.kafka.common.serialization.Deserializer;

public class VendaDeserializer implements Deserializer<Venda> {
    @Override
    public Venda deserialize(String s, byte[] bytes) {
        try {
            return new ObjectMapper().readValue(bytes, Venda.class);
        } catch (Exception e) {
            return null;
        }
    }
}
