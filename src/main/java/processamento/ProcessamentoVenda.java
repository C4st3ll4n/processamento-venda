package processamento;

import deserializer.VendaDeserializer;
import model.Venda;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class ProcessamentoVenda {
    public static void main(String[] args) throws InterruptedException {
        Properties _properties = new Properties();
        _properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "processing");
        _properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        _properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        _properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        _properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VendaDeserializer.class.getName());
        _properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        try (KafkaConsumer<String, Venda> _consumer = new KafkaConsumer<>(_properties)) {
                _consumer.subscribe(List.of("venda-ingresso"));
            while (true) {
                ConsumerRecords<String, Venda> _records = _consumer.poll(Duration.ofSeconds(2));
                for (var rec : _records) {
                    Venda _venda = rec.value();

                    if(new Random().nextBoolean()){
                        _venda.setStatus("APROVADA");
                    }else{

                        _venda.setStatus("REPROVADA");
                    }

                    Thread.sleep(500);
                    System.out.println(_venda);
                }

            }
        }
    }
}
