package net.soundvibe.lasher.map.model;

public enum FileType {

    INDEX("index.lasher"), DATA("data.lasher");

    public final String filename;

    FileType(String filename) {
        this.filename = filename;
    }
}
