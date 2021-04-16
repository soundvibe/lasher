package net.soundvibe.lasher.map.sync;

import org.junit.jupiter.api.*;

import java.util.concurrent.locks.ReentrantReadWriteLock;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RWLockerTest {

	@Test
	void should_read_lock() {
		var sut = new RWLocker(new ReentrantReadWriteLock());
		var readLock = sut.readLock();
		readLock.unlock();
	}
}
