import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PMO_UncaughtException implements Thread.UncaughtExceptionHandler {

	private Map<Thread, Throwable> log = Collections.synchronizedMap(new HashMap<>());

	// TODO timehelper - czas relative

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		PMO_SystemOutRedirect.println( "[zlapano wyjatek " + e.toString() + "]");
		log.put(t, e);
		e.printStackTrace();
	}

	public boolean isEmpty() {
		return log.isEmpty();
	}
	
	private String stackTrace2String( Throwable e ) {
		StringWriter errors = new StringWriter();
		e.printStackTrace(new PrintWriter(errors));
		return errors.toString();
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		log.entrySet().forEach(
				e -> {
					result.append("Watek " + e.getKey().getName() + " zglosil wyjatek " + e.getValue().toString() + "\n");
					result.append( stackTrace2String( e.getValue() ));
				});
		
		return result.toString();
	}

	public PMO_UncaughtException() {
		Thread.setDefaultUncaughtExceptionHandler(this);
	}
}
