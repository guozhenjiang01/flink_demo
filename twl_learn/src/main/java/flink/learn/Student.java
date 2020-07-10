package flink.learn;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author guozhenjiang
 * @description
 * @create 2020-06-27 19:23
 */
@AllArgsConstructor
@Data
public class Student {
	public int id;
	public String name;
	public String password;
	public int age;
}

