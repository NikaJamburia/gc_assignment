package ge.nika.georgian_card_assignment;

import java.math.BigDecimal;
import java.time.Instant;

public record MerchantStatistics(
        BigDecimal totalAmount,
        int transactionsCount,
        BigDecimal minAmount,
        BigDecimal maxAmount,
        BigDecimal avgAmount,
        Instant timestamp,
        String lastTransactionId
) {
}
