package cwh.posting.utils;

import java.util.HashMap;
import java.util.Map;


public class DataSetUtils {
	public static final Map<Integer, String> genderMap = new HashMap<>();
	public static final Map<Integer, String> teamMap = new HashMap<>();
	public static final Map<Integer, String> emailMap = new HashMap<>();
	
	static {
		genderMap.put(0, "남");
		genderMap.put(1, "여");
		
		teamMap.put(0, "롯데");
		teamMap.put(1, "한화");
		teamMap.put(2, "LG");
		teamMap.put(3, "NC");
		teamMap.put(4, "기아");
		teamMap.put(5, "넥센");
		teamMap.put(6, "두산");
		teamMap.put(7, "삼성");
		teamMap.put(8, "kt");
		teamMap.put(9, "SK");
		
		emailMap.put(0, "naver");
		emailMap.put(1, "daum");
		emailMap.put(2, "gmail");
	}
}
