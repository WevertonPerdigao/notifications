import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {


    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        try (var service = new KafkaService<>(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Order.class,
                Map.of()
        )) {
            service.run();
        }

    }

    private final KafkaDispatcherProducer<Order> kafkaDispatcherProducer = new KafkaDispatcherProducer<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("*__________________________________________*");
        System.out.println("Processando new Order, fkekfçdk");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();

        }

        var order = record.value();

        /* dectector de fraudes*/
        if (isFraud(order)) {
            System.out.println("Order is a fraud!!!" + order);
            kafkaDispatcherProducer.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
        } else {
            kafkaDispatcherProducer.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
        }


        System.out.println("Order processed");
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
