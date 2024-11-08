package net.soundvibe.lasher.serde;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class UUIDSerdeTest {

    private static final UUIDSerde sut = new UUIDSerde();

    @ParameterizedTest
    @NullSource
    @MethodSource("uuidProvider")
    void should_read_write_uuids(UUID uuid) {
        var bytes = sut.toBytes(uuid);
        var actual = sut.fromBytes(bytes);
        assertEquals(uuid, actual);
    }

    static Stream<UUID> uuidProvider() {
        return IntStream.range(0, 100)
                .mapToObj(i -> UUID.randomUUID());
    }

    static Stream<Integer> intProvider() {
        return IntStream.of(100, 125458745, 56958745, 1254_458, 1162).boxed();
    }

    @ParameterizedTest
    @MethodSource("intProvider")
    void should_find_solution(int n) {
        var solution = new Solution();
        var actual = solution.solution(n);
        System.out.println(actual);
    }

    @Test
    void should_find_solution_2() {
        var solution = new Solution2();
        var actual = solution.solution(new int[]{1, 2, 3, 4, 5}, 2);
        System.out.println(Arrays.toString(actual));
    }

    @Test
    void should_find_solution_3() {
        var solution = new Solution3();

        assertEquals(4, solution.solution(new int[]{2, 3, 1, 5}));
        assertEquals(3, solution.solution(new int[]{1, 2}));
        assertEquals(1, solution.solution(new int[]{2}));
        assertEquals(1, solution.solution(new int[]{}));
    }
}


class Solution {
    public int solution(int n) {
        // Implement your solution here
        var binary = Integer.toBinaryString(n);
        int maxLen = 0;
        System.out.println(String.format("n=%d, binary=%s", n, binary));
        for (int cursor = 0; cursor < binary.length(); cursor++) {
            char c = binary.charAt(cursor);
            //System.out.println(c);
            char cNext = ' ';
            if (cursor+1 < binary.length()) {
                cNext = binary.charAt(cursor+1);
            }

            if (c == '1' && cNext == '0') {
                // lookup until reached 1
                cursor+=2;
                int newMaxLen = 1;

                while (cursor < binary.length()) {
                    char searchChar = binary.charAt(cursor);
                    cursor++;
                    if (searchChar == '0') {
                        newMaxLen++;
                    } else {
                        maxLen = Math.max(maxLen, newMaxLen);
                        cursor-=2;
                        break;
                    }
                }
            }
        }
        return maxLen;
    }
}

// you can also use imports, for example:
// import java.util.*;

// you can write to stdout for debugging purposes, e.g.
// System.out.println("this is a debug message");

class Solution2 {
    public int[] solution(int[] a, int k) {
        if (a.length <= 1) {
            return a;
        }

        int[] dest = a;
        for (int i = 0; i < k; i++) {
            dest = shiftRight(dest);
        }

        return dest;
    }

    private int[] shiftRight(int[] src) {
        int[] dst = new int[src.length];
        dst[0] = src[src.length-1];
        System.arraycopy(src, 0, dst, 1, src.length - 1);
        return dst;
    }
}

class Solution3 {
    public int solution(int[] a) {
        if (a.length == 1) {
            return a[0] == 1 ? 2 : 1;
        } else if (a.length == 0) {
            return 1;
        }

        Arrays.sort(a);
        for (int i = 1; i <= a.length; i++) {
            if (a[i-1] != i) {
                return i;
            }
        }

        return a[a.length-1]+1;
    }
}