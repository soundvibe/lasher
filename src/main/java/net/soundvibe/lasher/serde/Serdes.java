package net.soundvibe.lasher.serde;

public interface Serdes {

    LongSerde LONG = new LongSerde();
    IntegerSerde INTEGER = new IntegerSerde();
    BytesSerde BYTES = new BytesSerde();
    StringSerde STRING = new StringSerde();
    UUIDSerde UUID = new UUIDSerde();

}
