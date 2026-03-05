package ge.nika.georgian_card_assignment;

import java.math.BigDecimal;
import java.time.Instant;

public record TransactionData(
        String merchantId,
        String transactionId,
        BigDecimal amount,
        Instant timestamp
) {
}
