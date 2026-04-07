import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Q8_YearCountryWritable implements WritableComparable<Q8_YearCountryWritable> {

    private String year;
    private String country;

    public Q8_YearCountryWritable() {
        this.year = "";
        this.country = "";
    }

    public Q8_YearCountryWritable(String year, String country) {
        this.year = year;
        this.country = country;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeUTF(country);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
        country = in.readUTF();
    }

    @Override
    public int compareTo(Q8_YearCountryWritable other) {
        int cmp = this.year.compareTo(other.year);
        if (cmp != 0) return cmp;
        return this.country.compareTo(other.country);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Q8_YearCountryWritable that = (Q8_YearCountryWritable) o;
        return year.equals(that.year) && country.equals(that.country);
    }

    @Override
    public int hashCode() {
        return year.hashCode() * 31 + country.hashCode();
    }

    @Override
    public String toString() {
        return year + "\t" + country;
    }
}
