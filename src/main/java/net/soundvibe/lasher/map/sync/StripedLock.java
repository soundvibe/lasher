package net.soundvibe.lasher.map.sync;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.*;

public final class StripedLock {

    private final List<Locker> locks;

    public StripedLock(int stripes) {
        this.locks = IntStream.range(0, stripes)
                .mapToObj(i -> new RWLocker(new ReentrantReadWriteLock()))
                .collect(Collectors.toList());
    }

    public Locker get(int index) {
        return locks.get(index);
    }
}
