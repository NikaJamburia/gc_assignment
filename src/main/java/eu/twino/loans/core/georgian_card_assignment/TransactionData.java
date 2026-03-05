package eu.twino.loans.core.georgian_card_assignment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

public record TransactionData(
        String merchantId,
        String transactionId,
        BigDecimal amount,
        Instant timestamp
) {
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        TransactionData that = (TransactionData) o;
        return Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(transactionId);
    }
}
