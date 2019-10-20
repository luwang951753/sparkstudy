package spark_1_6.sparksql;

import java.io.Serializable;

public class Student implements Serializable {

    public int id;
    public int age;
    public String name;

    public int getId() {
        return id;
    }

    public Student(String name,int id, int age) {
        this.id = id;
        this.age = age;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +

                ", age=" + age +
                ", name='" + name + '\'' +
                '}';
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
