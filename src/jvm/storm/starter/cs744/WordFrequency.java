package storm.starter.cs744;

import java.io.Serializable;

public class WordFrequency implements Serializable {

    private String word;
    private Long frequency;

    public WordFrequency() {
    }

    public WordFrequency(String word, Long frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getFrequency() {
        return frequency;
    }

    public void setFrequency(Long frequency) {
        this.frequency = frequency;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WordFrequency frequency1 = (WordFrequency) o;

        if (word != null ? !word.equals(frequency1.word) : frequency1.word != null)
            return false;
        return frequency != null ? frequency.equals(frequency1.frequency) : frequency1.frequency == null;
    }

    @Override
    public int hashCode() {
        int result = word != null ? word.hashCode() : 0;
        result = 31 * result + (frequency != null ? frequency.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WordFrequency{" +
                "word='" + word + '\'' +
                ", frequency=" + frequency +
                '}';
    }
}
