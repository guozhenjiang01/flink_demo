package action;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author guozhenjiang
 * @description
 * @create 2020-06-15 17:07
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BgActionBean {
	private String action;
	private String actionp;
	private String actionp2;
	private long timeStamp;
}
