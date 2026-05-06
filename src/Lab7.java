package lab7;

import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
public class Lab7 {

    private static final String RESET = "\u001B[0m";
    private static final String CYAN = "\u001B[36m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String RED = "\u001B[31m";
    private static final String PURPLE = "\u001B[35m";

    public static void main(String[] args) {
        printHeader("ЛАБОРАТОРНА РОБОТА №7: RxJava (ADVANCED EDITION)");

        try {
            executeTask("1.2 СЕРЕДНЬО: Порівняння підходів (Імперативний, Стріми, RxJava)", Lab7::task1_2);
            executeTask("2.2 СЕРЕДНЬО: Холодні та Гарячі Observables", Lab7::task2_2);
            executeTask("3.2 СЕРЕДНЬО: Оператори flatMap() vs concatMap()", Lab7::task3_2);
            executeTask("4.2 СЕРЕДНЬО: Спеціалізовані типи Maybe та Completable", Lab7::task4_2);
            executeTask("5.2 СЕРЕДНЬО: Паралельна агрегація даних з мікросервісів", Lab7::task5_2);
            executeTask("6.2 СЕРЕДНЬО: Контроль потоку (Backpressure DROP)", Lab7::task6_2);
            executeTask("7.2 СЕРЕДНЬО: Error Handling (Exponential Backoff)", Lab7::task7_2);
        } catch (Exception e) {
            System.err.println(RED + "Критична помилка виконання: " + e.getMessage() + RESET);
        }

        printHeader("ВСІ ЗАВДАННЯ ВИКОНАНО");
    }

    private static void task1_2() {
        List<String> cities = Arrays.asList(
                "Київ", "Харків", "Одеса", "Дніпро", "Запоріжжя",
                "Кривий Ріг", "Миколаїв", "Херсон", "Кропивницький",
                "Черкаси", "Суми", "Хмельницький", "Чернівці", "Каховка"
        );

        System.out.println(CYAN + "[Імперативний підхід]:" + RESET);
        java.util.Collections.sort(cities);
        for (String city : cities) {
            if (city.startsWith("К")) System.out.println("  " + city.toUpperCase());
        }

        System.out.println(CYAN + "\n[Функціональний підхід (Java Streams)]:" + RESET);
        cities.stream()
                .filter(c -> c.startsWith("К"))
                .map(String::toUpperCase)
                .sorted()
                .forEach(c -> System.out.println("  " + c));

        System.out.println(CYAN + "\n[Реактивний підхід (RxJava)]:" + RESET);
        Observable.fromIterable(cities)
                .filter(c -> c.startsWith("К"))
                .map(String::toUpperCase)
                .sorted()
                .subscribe(c -> System.out.println("  " + c));
    }

    private static void task2_2() throws Exception {
        List<String> results = Arrays.asList(
                "Динамо 2:1 Шахтар", "Шахтар 3:0 Металіст",
                "Ворскла 1:1 Зоря", "Дніпро-1 2:0 Львів", "Олександрія 0:1 Колос"
        );

        System.out.println(CYAN + "Частина А: Холодний Observable (повний повтор для кожного):" + RESET);
        Observable<String> cold = Observable.fromIterable(results);
        cold.subscribe(r -> System.out.println("  Глядач 1: " + r));
        cold.subscribe(r -> System.out.println("  Глядач 2: " + r));

        System.out.println(CYAN + "\nЧастина B: Гарячий Observable (трансляція наживо):" + RESET);
        CountDownLatch latch = new CountDownLatch(1);
        Observable<String> hotBase = Observable.interval(500, TimeUnit.MILLISECONDS)
                .map(i -> results.get(i.intValue()))
                .take(results.size());

        io.reactivex.rxjava3.observables.ConnectableObservable<String> hot = hotBase.publish();

        hot.subscribe(r -> System.out.println("  [ЕФІР] Глядач 1 отримав: " + r));
        hot.connect();

        Thread.sleep(1200); // Запізнення другого глядача
        hot.subscribe(r -> System.out.println("  [ЕФІР] Глядач 2 отримав: " + r),
                     Throwable::printStackTrace,
                     latch::countDown);

        latch.await(5, TimeUnit.SECONDS);
    }

    record FoodOrder(String orderId, List<String> items) {}

    private static void task3_2() throws Exception {
        List<FoodOrder> orders = Arrays.asList(
                new FoodOrder("ZAM-01", Arrays.asList("Піца Маргарита", "Кола 0.5л")),
                new FoodOrder("ZAM-02", Arrays.asList("Борщ", "Вареники", "Компот")),
                new FoodOrder("ZAM-03", Arrays.asList("Суші-сет 20шт", "Місо-суп"))
        );

        System.out.println(CYAN + "Частина A: flatMap() розгортання в єдиний потік:" + RESET);
        Observable.fromIterable(orders)
                .flatMapIterable(FoodOrder::items)
                .subscribe(item -> System.out.println("  >> " + item));

        System.out.println(CYAN + "\nЧастина B: Порівняння flatMap (паралельно) та concatMap (черга):" + RESET);

        System.out.print("  flatMap: ");
        CountDownLatch flatLatch = new CountDownLatch(1);
        Observable.fromIterable(orders)
                .flatMap(o -> Observable.fromIterable(o.items()).delay(200, TimeUnit.MILLISECONDS))
                .subscribe(item -> System.out.print(item + " | "), Throwable::printStackTrace, flatLatch::countDown);

        flatLatch.await(5, TimeUnit.SECONDS);

        System.out.print("\n  concatMap: ");
        CountDownLatch concatLatch = new CountDownLatch(1);
        Observable.fromIterable(orders)
                .concatMap(o -> Observable.fromIterable(o.items()).delay(200, TimeUnit.MILLISECONDS))
                .subscribe(item -> System.out.print(item + " | "), Throwable::printStackTrace, concatLatch::countDown);

        concatLatch.await(5, TimeUnit.SECONDS);
        System.out.println();
    }

    private static void task4_2() {
        System.out.println(CYAN + "Частина A: Реактивний кеш (Maybe):" + RESET);
        String[] keys = {"user:1", "user:2", "user:error"};
        for (String key : keys) {
            findInCache(key)
                    .defaultIfEmpty("Завантажено з бази даних (Cache Miss)")
                    .subscribe(
                        val -> System.out.println("  [" + key + "] Result: " + val),
                        err -> System.err.println("  [" + key + "] Error: " + err.getMessage())
                    );
        }

        System.out.println(CYAN + "\nЧастина B: Ланцюжок реєстрації (Completable):" + RESET);
        validateInput()
                .andThen(saveToDatabase(true))
                .andThen(generateToken())
                .subscribe(
                    token -> System.out.println(GREEN + "  (+) Процес завершено. Токен видано: " + token + RESET),
                    err -> System.err.println(RED + "  (-) Помилка ланцюжка: " + err.getMessage() + RESET)
                );
    }

    // --- 5. АСИНХРОННІ ОПЕРАЦІЇ ТА SCHEDULERS ---
    record ServiceCall(String serviceName, int delayMs) {}

    private static void task5_2() {
        List<ServiceCall> services = Arrays.asList(
                new ServiceCall("AuthService", 800),
                new ServiceCall("OrderService", 1200),
                new ServiceCall("PromoService", 600)
        );

        System.out.println(CYAN + "Порівняння швидкості агрегації:" + RESET);

        long startSync = System.currentTimeMillis();
        Observable.fromIterable(services)
                .concatMap(s -> Observable.just(s).delay(s.delayMs(), TimeUnit.MILLISECONDS))
                .blockingSubscribe(s -> System.out.println("  [SYNC] " + s.serviceName + " готовий"));
        System.out.println(YELLOW + "  Загальний час (послідовно): " + (System.currentTimeMillis() - startSync) + " мс" + RESET);

        long startAsync = System.currentTimeMillis();
        Observable.fromIterable(services)
                .flatMap(s -> Observable.just(s)
                        .subscribeOn(Schedulers.io())
                        .delay(s.delayMs(), TimeUnit.MILLISECONDS)
                        .doOnNext(sc -> System.out.println("  [ASYNC] " + sc.serviceName + " готовий (Thread: " + Thread.currentThread().getName() + ")")))
                .blockingSubscribe();
        System.out.println(GREEN + "  Загальний час (паралельно): " + (System.currentTimeMillis() - startAsync) + " мс" + RESET);
    }

    private static void task6_2() throws Exception {
        System.out.println(CYAN + "Частина A: Batch Processing (buffer):" + RESET);
        Observable.range(1, 12)
                .map(i -> "Event-" + i)
                .buffer(5)
                .subscribe(batch -> System.out.println("  [DATABASE] Batch INSERT: " + batch));

        System.out.println(CYAN + "\nЧастина B: BackpressureStrategy.DROP (утилізація зайвого):" + RESET);
        AtomicInteger processed = new AtomicInteger();
        AtomicInteger dropped = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);

        Flowable.range(1, 1000)
                .onBackpressureDrop(i -> dropped.incrementAndGet())
                .observeOn(Schedulers.computation(), false, 1) // Обмежуємо буфер до 1 для наочності DROP
                .subscribe(new Subscriber<Integer>() {
                    private Subscription s;
                    @Override public void onSubscribe(Subscription s) { this.s = s; s.request(1); }
                    @Override public void onNext(Integer i) {
                        try { Thread.sleep(5); } catch (InterruptedException e) {} // Повільна обробка
                        processed.incrementAndGet();
                        s.request(1);
                    }
                    @Override public void onError(Throwable t) {}
                    @Override public void onComplete() { latch.countDown(); }
                });

        latch.await(3, TimeUnit.SECONDS);
        System.out.println(PURPLE + "  [RESULT] Оброблено: " + processed.get() + " | Відкинуто: " + dropped.get() + RESET);
    }

    private static void task7_2() throws Exception {
        System.out.println(CYAN + "Exponential Backoff (Повтор запиту із затримкою 2^(n-1)):" + RESET);
        AtomicInteger attempt = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        Observable<String> unstableApi = Observable.create(emitter -> {
            int current = attempt.incrementAndGet();
            System.out.println("  [SYSTEM] Спроба #" + current + "...");
            if (current < 4) {
                emitter.onError(new IOException("Timeout"));
            } else {
                emitter.onNext(GREEN + "  (+) Дані успішно отримано на спробі " + current + RESET);
                emitter.onComplete();
            }
        });

        unstableApi.retryWhen(errors -> errors
                .zipWith(Observable.range(1, 4), (err, retry) -> retry)
                .flatMap(retry -> {
                    long delay = (long) Math.pow(2, retry - 1);
                    System.out.println(YELLOW + "    ! Помилка. Очікуємо " + delay + " сек..." + RESET);
                    return Observable.timer(delay, TimeUnit.SECONDS);
                }))
                .subscribe(System.out::println, Throwable::printStackTrace, latch::countDown);

        latch.await(15, TimeUnit.SECONDS);
    }

    // --- ДОПОМІЖНІ МЕТОДИ ---

    private static Maybe<String> findInCache(String key) {
        return switch (key) {
            case "user:1" -> Maybe.just("{'id':1, 'name':'Леся'}");
            case "user:error" -> Maybe.error(new RuntimeException("Redis connection refused"));
            default -> Maybe.empty();
        };
    }

    private static Completable validateInput() {
        return Completable.fromAction(() -> System.out.println("  [VALIDATE] Перевірка вхідних даних..."));
    }

    private static Completable saveToDatabase(boolean success) {
        return Completable.fromAction(() -> {
            System.out.println("  [DB] Збереження профілю користувача...");
            if (!success) throw new IOException("Database error");
        });
    }

    private static Single<String> generateToken() {
        return Single.just("SECURE-JWT-TOKEN-2026").delay(100, TimeUnit.MILLISECONDS);
    }

    private static void printHeader(String title) {
        String border = "=".repeat(title.length() + 10);
        System.out.println("\n" + PURPLE + border + RESET);
        System.out.println(PURPLE + "     " + title + RESET);
        System.out.println(PURPLE + border + "\n" + RESET);
    }

    private interface Task { void run() throws Exception; }

    private static void executeTask(String name, Task task) throws Exception {
        System.out.println(YELLOW + ">>> ВИКОНУЄТЬСЯ " + name + RESET);
        task.run();
        System.out.println(PURPLE + "-".repeat(40) + RESET + "\n");
    }
}
