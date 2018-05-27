import java.util.Set;
import java.util.TreeSet;

public class PMO_UniqStringGenerator {
	private static Set<String> history = new TreeSet<>();

	public synchronized static String get() {
		String tmp;
		do {
			tmp = Long.toHexString(Double.doubleToLongBits(Math.random()));
		} while (!history.add(tmp));
		return tmp;
	}
	
	public static void main(String[] args) {
		System.out.println( PMO_UniqStringGenerator.get() );
		System.out.println( PMO_UniqStringGenerator.get() );
		System.out.println( PMO_UniqStringGenerator.get() );
		System.out.println( PMO_UniqStringGenerator.get() );
	}
}
