
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

//publish
public class NewOrderMain {


    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try (var orderDispatcher = new KafkaDispatcherProducer<Order>()) {
            try (var emailDispatcher = new KafkaDispatcherProducer<String>()) {
                var orderId = UUID.randomUUID().toString();
                var amount = new BigDecimal(Math.random() * 5000 + 1);
                var email = Math.random() + "@email.com";

                var order = new Order(orderId, amount, email);

                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

                var emailCode = "Thank you for your order! We are processing your order!";

                emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);

            }
        }
    }


}

