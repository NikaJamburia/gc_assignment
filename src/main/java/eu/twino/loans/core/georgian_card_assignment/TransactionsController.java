package eu.twino.loans.core.georgian_card_assignment;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.*;
import java.util.stream.Collectors;

/**
 * ტესტებს ვეღარ ვასწრებ, შეიძლება რაღაცეებმა არ იმუშაუს, მარა ლოგიკა მგონი გასაგებია :)
 *
 * პროგრამა აგგრეგაციას აკეთებს ყოველი ტრანბზაქციის მერე, კონკრეტული მერჩანთისთვის.
 * გამოდის რომ ყველა მერჩანთს აქვს სტატისტიკის რამდენიმე ჩანაწერი თაიმსტემპით, თუ როდის იყო შექმნილი
 * თუ გვინდა ბოლო 60 წამის სტატისტიკის აღებას, ვიღებთ ბოლო 60 წამის განმავლობაში დამატებულ ყველა ჩანაწერს, და ვირჩევთ ყველაზე ახალს
 * თუ რეინჯით ვიღებთ, უბრალოდ ვფილტრავთ და ასევე ბოლო ჩანაწერს ვიღებთ
 *
 * ყოველ 5 წუთში ეშვება შედულერი, რომელიც შლის მოძველებულ ტრანზაქციებს.
 */
@RestController
public class TransactionsController {

    private final Map<String, Set<MerchantStatistics>> merchantsStatistics = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final Lock lock = new ReentrantLock();
    private final Condition statisticsCleared = lock.newCondition();
    private final AtomicBoolean clearingInProgress = new AtomicBoolean(false);


    @EventListener(ApplicationReadyEvent.class)
    public void startBackgroundThread() {
        scheduler.scheduleAtFixedRate(() -> {
            var fiveMinsAgo = Instant.now().minusSeconds(60 * 5);

            lock.lock();
            try {
                clearingInProgress.set(true);
                merchantsStatistics.forEach((key, value) -> {
                    var filtered = value
                            .stream()
                            .filter(st -> st.timestamp().isAfter(fiveMinsAgo))
                            .collect(Collectors.toCollection(HashSet::new));
                    merchantsStatistics.put(key, filtered);
                });
            } finally {
                clearingInProgress.set(false);
                statisticsCleared.signal();
                lock.unlock();
            }
        }, 5, 5, TimeUnit.MINUTES);
    }

    @PostMapping("/transactions")
    void insertTransaction(@RequestBody TransactionData transactionData) {
        if (Duration.between(transactionData.timestamp(), Instant.now()).getSeconds() > 60) {
            throw new IllegalStateException("Transaction too old");
        }

        // ვააფდეითებთ სტატისტიკას ასინქრონულად. სინქრონულადაც შეიძლება, მაგრამ ვების ტრედს მოუწევდა დალოდებოდა სტატისტიკის განახლებას.
        executor.submit(() -> updateStatistics(transactionData));
    }

    @GetMapping("/stats")
    MerchantStatisticsResponse getStats(@RequestParam("merchantId") String merchantId) {
        return getMerchantStatisticsResponse(merchantId, Instant.now().minusSeconds(60), Instant.now());
    }

    @GetMapping("/stats/range")
    MerchantStatisticsResponse getStatsRanged(
            @RequestParam("merchantId") String merchantId,
            @RequestParam("from") Instant from,
            @RequestParam("to") Instant to
    ) {
        return getMerchantStatisticsResponse(merchantId, from, to);
    }

    private MerchantStatisticsResponse getMerchantStatisticsResponse(String merchantId, Instant from, Instant to) {
        return merchantsStatistics.get(merchantId)
                .stream()
                .filter(stat ->
                        stat.timestamp().isAfter(from) && stat.timestamp().isBefore(to)
                )
                .max(Comparator.comparing(MerchantStatistics::timestamp))
                .map(stat -> new MerchantStatisticsResponse(
                        stat.totalAmount(),
                        stat.transactionsCount(),
                        stat.minAmount(),
                        stat.maxAmount(),
                        stat.avgAmount()
                ))
                .orElseThrow(() -> new IllegalArgumentException("No statistics for merchant"));
    }

    private void updateStatistics(TransactionData transactionData) {
        // ველოდებით ძველი სტატისტიკის წაშლას
        lock.lock();

        try {
            while (clearingInProgress.get()) {
                statisticsCleared.await();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        merchantsStatistics.computeIfPresent(transactionData.merchantId(),( key, value) -> {
            if (value.stream().map(MerchantStatistics::lastTransactionId).toList().contains(transactionData.merchantId())) {
                return value;
            }
            var lastRecord = value
                    .stream()
                    .max(Comparator.comparing(MerchantStatistics::timestamp))
                    .orElseThrow();
            BigDecimal newTotal = lastRecord.totalAmount().add(transactionData.amount());
            int newCount = lastRecord.transactionsCount() + 1;

            var newStat = new MerchantStatistics(
                    newTotal,
                    newCount,
                    transactionData.amount().min(lastRecord.minAmount()),
                    transactionData.amount().max(lastRecord.maxAmount()),
                    newTotal.divide(new BigDecimal(newCount), RoundingMode.HALF_DOWN),
                    transactionData.timestamp(),
                    transactionData.transactionId()
            );
            value.add(newStat);
            return value;
        });

        merchantsStatistics.computeIfAbsent(transactionData.merchantId(), (key) -> {
            var newStat = new MerchantStatistics(
                    transactionData.amount(),
                    1,
                    transactionData.amount(),
                    transactionData.amount(),
                    transactionData.amount(),
                    transactionData.timestamp(),
                    transactionData.transactionId()
            );
            Set<MerchantStatistics> value = new HashSet<>();
            value.add(newStat);
            return value;
        });
    }
}
