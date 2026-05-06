package lab7;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.disposables.Disposable;
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

    public static void main(String[] args) throws Exception {
        task1_1();
        task1_2();
        task2_1();
        task2_2();
        task3_1();
        task3_2();
        task4_1();
        task4_2();
        task5_1();
        task5_2();
        task6_1();
        task6_2();
        task7_1();
        task7_2();
    }

    enum OrderStatus { DELIVERED, PENDING, CANCELLED }
    record Order(String id, OrderStatus status, double amount) {}

    private static void task1_1() {
        List<Order> orders = Arrays.asList(
                new Order("O-001", OrderStatus.DELIVERED, 1500.00),
                new Order("O-002", OrderStatus.PENDING, 300.00),
                new Order("O-003", OrderStatus.CANCELLED, 75.00),
                new Order("O-004", OrderStatus.DELIVERED, 2200.00),
                new Order("O-005", OrderStatus.PENDING, 450.00),
                new Order("O-006", OrderStatus.DELIVERED, 980.00)
        );

        long count = orders.stream()
                .filter(o -> o.status() == OrderStatus.DELIVERED)
                .count();

        double sum = orders.stream()
                .filter(o -> o.status() == OrderStatus.DELIVERED)
                .mapToDouble(Order::amount)
                .sum();

        System.out.println("Виконаних замовлень: " + count);
        System.out.println("Загальна сума: " + sum);
    }

    private static void task1_2() {
        List<String> cities = Arrays.asList(
                "Київ", "Харків", "Одеса", "Дніпро", "Запоріжжя",
                "Кривий Ріг", "Миколаїв", "Херсон", "Кропивницький",
                "Черкаси", "Суми", "Хмельницький", "Чернівці", "Каховка"
        );

        System.out.println("\n[Імперативний]:");
        List<String> sorted = new java.util.ArrayList<>(cities);
        java.util.Collections.sort(sorted);
        for (String c : sorted) {
            if (c.startsWith("К")) System.out.println(c.toUpperCase());
        }

        System.out.println("\n[Стріми]:");
        cities.stream()
                .filter(c -> c.startsWith("К"))
                .map(String::toUpperCase)
                .sorted()
                .forEach(System.out::println);

        System.out.println("\n[RxJava]:");
        Observable.fromIterable(cities)
                .filter(c -> c.startsWith("К"))
                .map(String::toUpperCase)
                .sorted()
                .subscribe(System.out::println);
    }

    private static void task2_1() {
        System.out.println();
        Observable<String> atm = Observable.just(
                "Вставте картку",
                "Введіть PIN-код",
                "Оберіть суму: 500 грн",
                "Видача готівки…",
                "Дякуємо! Заберіть картку"
        );

        atm.subscribe(new Observer<String>() {
            @Override public void onSubscribe(@NonNull Disposable d) { System.out.println("[БАНКОМАТ] Сесію розпочато"); }
            @Override public void onNext(@NonNull String s) { System.out.println(">> " + s); }
            @Override public void onError(@NonNull Throwable e) { System.out.println("[БАНКОМАТ] Помилка: " + e.getMessage()); }
            @Override public void onComplete() { System.out.println("[БАНКОМАТ] Сесію завершено"); }
        });
    }

    private static void task2_2() throws Exception {
        List<String> matches = Arrays.asList(
                "Динамо 2:1 Шахтар", "Шахтар 3:0 Металіст",
                "Ворскла 1:1 Зоря", "Дніпро-1 2:0 Львів", "Олександрія 0:1 Колос"
        );

        System.out.println("\nХолодний Observable:");
        Observable<String> cold = Observable.fromIterable(matches);
        cold.subscribe(r -> System.out.println("Глядач 1: " + r));
        cold.subscribe(r -> System.out.println("Глядач 2: " + r));

        System.out.println("\nГарячий Observable:");
        CountDownLatch latch = new CountDownLatch(1);
        Observable<String> hotBase = Observable.interval(500, TimeUnit.MILLISECONDS)
                .map(i -> matches.get(i.intValue()))
                .take(matches.size());

        var hot = hotBase.publish();
        hot.subscribe(r -> System.out.println("[LIVE] Глядач 1: " + r));
        hot.connect();

        Thread.sleep(1200);
        hot.subscribe(r -> System.out.println("[LIVE] Глядач 2 (запізнився): " + r),
                     e -> {},
                     latch::countDown);

        latch.await(5, TimeUnit.SECONDS);
    }

    record Product(String name, double priceUsd) {}

    private static void task3_1() {
        List<Product> products = Arrays.asList(
                new Product("Навушники Sony", 49.99),
                new Product("Клавіатура Logitech", 129.00),
                new Product("Монітор LG 27\"", 399.00),
                new Product("USB-хаб Anker", 35.00),
                new Product("Веб-камера Logitech", 149.00),
                new Product("Килимок для миші", 18.00),
                new Product("SSD Samsung 1TB", 110.00)
        );

        System.out.println();
        Observable.fromIterable(products)
                .filter(p -> p.priceUsd() > 100)
                .map(p -> p.name() + " -- " + String.format(java.util.Locale.US, "%.2f", p.priceUsd() * 41.5) + " грн (є в наявності)")
                .subscribe(System.out::println);
    }

    record FoodOrder(String orderId, List<String> items) {}

    private static void task3_2() throws Exception {
        List<FoodOrder> orders = Arrays.asList(
                new FoodOrder("ZAM-01", Arrays.asList("Піца Маргарита", "Кола 0.5л")),
                new FoodOrder("ZAM-02", Arrays.asList("Борщ", "Вареники", "Компот")),
                new FoodOrder("ZAM-03", Arrays.asList("Суші-сет 20шт", "Місо-суп"))
        );

        System.out.println("\nflatMap:");
        Observable.fromIterable(orders)
                .flatMapIterable(FoodOrder::items)
                .subscribe(item -> System.out.println(">> " + item));

        System.out.println("\nflatMap vs concatMap:");
        CountDownLatch l1 = new CountDownLatch(1);
        Observable.fromIterable(orders)
                .flatMap(o -> Observable.fromIterable(o.items()).delay(200, TimeUnit.MILLISECONDS))
                .subscribe(i -> System.out.print(i + " | "), e -> {}, l1::countDown);
        l1.await(3, TimeUnit.SECONDS);

        System.out.print("\n");
        CountDownLatch l2 = new CountDownLatch(1);
        Observable.fromIterable(orders)
                .concatMap(o -> Observable.fromIterable(o.items()).delay(200, TimeUnit.MILLISECONDS))
                .subscribe(i -> System.out.print(i + " | "), e -> {}, l2::countDown);
        l2.await(3, TimeUnit.SECONDS);
        System.out.println();
    }

    private static void task4_1() {
        System.out.println();
        getUserById(42).subscribe(
                u -> System.out.println("(+) Знайдено: " + u),
                e -> System.out.println("(-) Помилка: " + e.getMessage())
        );
        getUserById(-1).subscribe(
                u -> System.out.println("(+) Знайдено: " + u),
                e -> System.out.println("(-) Помилка: " + e.getMessage())
        );
    }

    private static Single<String> getUserById(int id) {
        if (id > 0) return Single.just("Користувач #" + id + ": Іван Франко");
        else return Single.error(new IllegalArgumentException("ID не може бути від’ємним або нульовим"));
    }

    private static void task4_2() {
        System.out.println("\nMaybe Cache:");
        List.of("user:1", "user:2", "user:error").forEach(k -> 
            findInCache(k)
                .defaultIfEmpty("Завантажено з БД (Cache Miss)")
                .subscribe(
                    v -> System.out.println("[" + k + "] -> " + v),
                    e -> System.out.println("[" + k + "] -> Помилка: " + e.getMessage())
                )
        );

        System.out.println("\nCompletable Chain:");
        validateInput()
                .andThen(saveToDatabase(true))
                .andThen(generateToken())
                .subscribe(
                    t -> System.out.println("(+) Реєстрація успішна. Token: " + t),
                    e -> System.out.println("(-) Помилка: " + e.getMessage())
                );
    }

    private static void task5_1() {
        System.out.println();
        Observable.just("photo_1.jpg", "photo_2.jpg")
                .doOnNext(img -> System.out.println("[" + Thread.currentThread().getName() + "] [ЗАВАНТ] Завантаження: " + img))
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .doOnNext(img -> System.out.println("[" + Thread.currentThread().getName() + "] [СТИСК] Стиснення: " + img))
                .observeOn(Schedulers.trampoline())
                .blockingSubscribe(img -> System.out.println("[" + Thread.currentThread().getName() + "] [ФОТО] Відображення: " + img));
    }

    record ServiceCall(String name, int delay) {}

    private static void task5_2() {
        List<ServiceCall> services = Arrays.asList(
                new ServiceCall("UserService", 800),
                new ServiceCall("OrderService", 1200),
                new ServiceCall("RecommendationService", 600)
        );

        System.out.println();
        long start = System.currentTimeMillis();
        Observable.fromIterable(services)
                .flatMap(s -> Observable.just(s)
                        .subscribeOn(Schedulers.io())
                        .delay(s.delay(), TimeUnit.MILLISECONDS)
                        .doOnNext(sc -> System.out.println("[OK] " + sc.name() + " (" + Thread.currentThread().getName() + ")")))
                .blockingSubscribe();
        System.out.println("Час: " + (System.currentTimeMillis() - start) + " мс");
    }

    private static void task6_1() throws Exception {
        Observable<String> keys = Observable.create(emitter -> {
            String[] inputs = {"К", "Ки", "Киї", "Київ", "Київ ", "Київ К", "Київ Ки"};
            long[] delays = { 50, 80, 120, 100, 400, 60, 350 };
            for (int i = 0; i < inputs.length; i++) {
                Thread.sleep(delays[i]);
                emitter.onNext(inputs[i]);
            }
            emitter.onComplete();
        });

        System.out.println();
        keys.debounce(300, TimeUnit.MILLISECONDS)
                .subscribe(s -> System.out.println("[ПОШУК] Запит: \"" + s + "\""));
    }

    private static void task6_2() throws Exception {
        System.out.println("\nbuffer(5):");
        Observable.fromArray("LOGIN:user1", "CLICK:btn_buy", "VIEW:product_42", "LOGIN:user2", "LOGOUT:user1", "CLICK:btn_cart", "VIEW:product_7", "LOGIN:user3", "CLICK:btn_pay", "LOGOUT:user2", "LOGIN:user4", "VIEW:product_1")
                .buffer(5)
                .subscribe(b -> System.out.println("[DB] Batch: " + b));

        System.out.println("\nFlowable.DROP:");
        AtomicInteger proc = new AtomicInteger();
        AtomicInteger drop = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);

        Flowable.range(1, 1000)
                .onBackpressureDrop(i -> drop.incrementAndGet())
                .observeOn(Schedulers.computation(), false, 128)
                .subscribe(new Subscriber<Integer>() {
                    private Subscription s;
                    @Override public void onSubscribe(Subscription s) { this.s = s; s.request(1); }
                    @Override public void onNext(Integer i) {
                        try { Thread.sleep(10); } catch (Exception e) {}
                        proc.incrementAndGet();
                        s.request(1);
                    }
                    @Override public void onError(Throwable t) {}
                    @Override public void onComplete() { latch.countDown(); }
                });

        latch.await(5, TimeUnit.SECONDS);
        System.out.println("Оброблено: " + proc.get() + " | Відкинуто: " + drop.get());
    }

    private static void task7_1() {
        Observable<String> service = Observable.create(emitter -> {
            emitter.onNext("USD -> UAH: 41.50");
            emitter.onNext("EUR -> UAH: 44.20");
            emitter.onError(new RuntimeException("Error"));
        });

        System.out.println("\nonErrorReturn:");
        service.onErrorReturn(e -> "Курс з кешу: USD -> UAH: 41.00")
                .subscribe(System.out::println);

        System.out.println("\nonErrorResumeNext:");
        service.onErrorResumeNext(e -> Observable.just("JPY -> UAH: 0.27", "PLN -> UAH: 10.30"))
                .subscribe(System.out::println);
    }

    private static void task7_2() throws Exception {
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        Observable<String> api = Observable.create(emitter -> {
            int a = count.incrementAndGet();
            System.out.println("Спроба #" + a);
            if (a < 4) emitter.onError(new IOException());
            else {
                emitter.onNext("(+) OK: {status: 'ok'}");
                emitter.onComplete();
            }
        });

        System.out.println();
        api.retryWhen(errs -> errs
                .zipWith(Observable.range(1, 4), (e, i) -> i)
                .flatMap(i -> {
                    long d = (long) Math.pow(2, i - 1);
                    System.out.println("Чекаємо " + d + " сек...");
                    return Observable.timer(d, TimeUnit.SECONDS);
                }))
                .subscribe(System.out::println, e -> {}, latch::countDown);

        latch.await(15, TimeUnit.SECONDS);
    }

    private static Maybe<String> findInCache(String k) {
        return switch (k) {
            case "user:1" -> Maybe.just("{'name':'Леся', 'age':28}");
            case "user:error" -> Maybe.error(new RuntimeException("Redis error"));
            default -> Maybe.empty();
        };
    }

    private static Completable validateInput() {
        return Completable.fromAction(() -> System.out.println("[ПОШУК] Перевірка..."));
    }

    private static Completable saveToDatabase(boolean s) {
        return Completable.fromAction(() -> {
            System.out.println("[DB] Збереження...");
            if (!s) throw new IOException();
        });
    }

    private static Single<String> generateToken() {
        return Single.just("jwt.token.demo");
    }
}
