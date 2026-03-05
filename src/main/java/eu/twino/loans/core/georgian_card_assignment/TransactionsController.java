package eu.twino.loans.core.georgian_card_assignment;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@RestController
// ტესტებს ვეღარ ვასწრებ, შეიძლება რაღაცეებმა არ იმუშაუს, მარა ლოგიკა მგონი გასაგებია :)
public class TransactionsController {

    private static final int STATISTICS_THRESHOLD = 10;

    private final List<TransactionData> savedTransactions = new ArrayList<>();
    private final Map<String, List<MerchantStatistics>> merchantsStatistics = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Condition merchantsStatisticsFull = lock.writeLock().newCondition();

    private final Thread backgroundStatisticsCleaningThread = new Thread(() -> {
        while (true) {
            try {
                // იცდის სანამ რომელიმე მერჩანთის სტატისტიკა 10ზე მეტი იქნება
                merchantsStatisticsFull.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            var writeLock = lock.writeLock();
            writeLock.lock();
            try {
                // ყველა მერჩანთს, რომლის სტატისტიკის რექორდები 10ზე მეტია, უშლის წინებს და ტოვებს მარტო ბოლოს
                merchantsStatistics.forEach((key, value) -> {
                    if (value.size() > STATISTICS_THRESHOLD) {
                        var latestStat = value.stream()
                                .max(Comparator.comparing(MerchantStatistics::timestamp))
                                .orElseThrow();
                        merchantsStatistics.put(key, List.of(latestStat));
                    }
                });
            } finally {
                writeLock.unlock();
            }
        }
    });

    @EventListener(ApplicationReadyEvent.class)
    public void doSomethingAfterStartup() {
        backgroundStatisticsCleaningThread.start();
    }

    @PostMapping("/transactions")
    void insertTransaction(@RequestBody TransactionData transactionData) {
        if (savedTransactions.stream().anyMatch(tx ->
                Objects.equals(tx.transactionId(), transactionData.transactionId()))) {
            return;
        }

        if (Duration.between(transactionData.timestamp(), Instant.now()).getSeconds() > 60) {
            throw new IllegalStateException("Transaction too old");
        }

        synchronized (this) {
            // ეს რეალურად არაა საჭირო :) თავიდან მქონდა ეს ლისტი როცა ჯერ ვერ ჩამოვყალიბდი სოლუშენზე
            savedTransactions.add(transactionData);
        }

        // ვააფდეითებთ სტატისტიკას ასინქრონულად
        executor.submit(() -> {
            updateStatistics(transactionData);
        });
    }

    @GetMapping("/stats")
    MerchantStatistics getStats(@RequestParam("merchantId") String merchantId) {
        // აქ გვინდა რიდ ლოქი, რომ არ ველოდოთ სტატისტიკის განახლების ტასკს და გვქონდეს ბოლო განახლების რეზულტატი
        var readLock = lock.readLock();
        try {
            return merchantsStatistics.get(merchantId)
                    .stream()
                    .filter(stat -> stat.timestamp().isAfter(Instant.now().minusSeconds(60)))
                    .max(Comparator.comparing(MerchantStatistics::timestamp))
                    .orElseThrow(() ->new IllegalArgumentException("No statistics for merchant"));
        } finally {
            readLock.unlock();
        }

    }

    @GetMapping("/stats/range")
    MerchantStatistics getStatsRanged(
            @RequestParam("merchantId") String merchantId,
            @RequestParam("from") Instant from,
            @RequestParam("to") Instant to
    ) {
        // აქ გვინდა რიდ ლოქი, რომ არ ველოდოთ სტატისტიკის განახლების ტასკს და გვქონდეს ბოლო განახლების რეზულტატი
        var readLock = lock.readLock();
        try {
            return merchantsStatistics.get(merchantId)
                    .stream()
                    .filter(stat ->
                            stat.timestamp().isAfter(from) && stat.timestamp().isBefore(to)
                    )
                    .max(Comparator.comparing(MerchantStatistics::timestamp))
                    .orElseThrow(() ->new IllegalArgumentException("No statistics for merchant"));
        } finally {
            readLock.unlock();
        }
    }

    private void updateStatistics(TransactionData transactionData) {
        // ვიღებთ ლოკს
        var writeLock = lock.writeLock();
        writeLock.lock();

        try {
            var stats = merchantsStatistics.get(transactionData.merchantId());

            if (stats == null) {
                var newStat = new MerchantStatistics(
                        transactionData.amount(),
                        1,
                        transactionData.amount(),
                        transactionData.amount(),
                        transactionData.amount(),
                        transactionData.timestamp()
                );
                merchantsStatistics.put(transactionData.merchantId(), List.of(newStat));
            } else {
                var lastRecord = stats
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
                        transactionData.timestamp()
                );
                stats.add(newStat);
                if (stats.size() > STATISTICS_THRESHOLD) {
                    // თუ ძაან ბევრი სტატისტიკა დაგროვდა მერჩანტზე, სიგნალს ვუგზავნით ბექრაუნდ ტრედს, რომელიც წაშლის
                    merchantsStatisticsFull.signal();
                }
            }
        } finally {
            writeLock.unlock();
        }

    }

//    private MerchantStatistics calculateStats(List<TransactionData> transactionData) {
//        BigDecimal totalAmount = new BigDecimal(0);
//        int count = 0;
//        BigDecimal min = null;
//        BigDecimal max = BigDecimal.ZERO;
//
//        for (TransactionData tx : transactionData) {
//            count++;
//            totalAmount = totalAmount.add(tx.amount());
//
//            if (min == null) {
//                min = tx.amount();
//            } else {
//                min = tx.amount().min(min);
//            }
//            max = tx.amount().max(tx.amount());
//        }
//
//        return new MerchantStatistics(
//            totalAmount,
//            count,
//            min,
//            max,
//            totalAmount.divide(new BigDecimal(count), RoundingMode.HALF_DOWN)
//        );
//    }
}
