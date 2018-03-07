package storm.starter.cs744;

import java.io.Serializable;

public class WordFrequency implements Serializable {

    private String word;
    private Integer frequency;

    public WordFrequency() {
    }

    public WordFrequency(String word, Integer frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "WordFrequency{" +
                "word='" + word + '\'' +
                ", frequency=" + frequency +
                '}';
    }
}
