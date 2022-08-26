package net.soundvibe.lasher.map.sync;

import org.junit.jupiter.api.*;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RWLockerTest {

	@Test
	void should_read_lock() {
		var sut = new RWLocker(new ReentrantReadWriteLock());
		sut.readLock();
		sut.readLock();
		sut.readUnlock();
		sut.readUnlock();

		assertTrue(true);
	}
}
