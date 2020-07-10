package flink.socket;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author guozhenjiang
 * @description
 * @create 2020-06-26 20:28
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class WordCount {
	public String word;
	public long count;
}
