package hello;

import java.sql.Date;

public class Transformed {
    private String name;
    private Date dateCreated;

    public Transformed() {
    }

    public Transformed(String name, Date dateCreated) {
        this.name = name;
        this.dateCreated = dateCreated;
    }

    public String getName() {
        return name;
    }

    public Date getDateCreated() {
        return dateCreated;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDateCreated(Date dateCreated) {
        this.dateCreated = dateCreated;
    }

    @Override
    public String toString() {
        return "Transformed{" +
                "name='" + name + '\'' +
                ", dateCreated=" + dateCreated +
                '}';
    }
}
