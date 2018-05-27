import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

public class PMO_ThreadsHelper {
	public static void joinThreads(List<Thread> ths) {
		ths.forEach((t) -> {
			try {
				t.join();
			} catch (InterruptedException ie) {
				PMO_SystemOutRedirect.println("Doszlo do wyjatku w trakcie join");
			}
		});
	}

	public static List<Thread> createAndStartThreads(Collection<? extends Runnable> tasks, boolean daemon) {
		List<Thread> result = new ArrayList<>();

		tasks.forEach(t -> {
			result.add(new Thread(t));
		});

		if (daemon) {
			result.forEach(t -> t.setDaemon(true));
		}

		result.forEach(t -> t.start());

		return result;
	}

	public static boolean wait(Object o) {
		if (o != null) {
			synchronized (o) {
				try {
					o.wait();
					return true;
				} catch (InterruptedException ie) {
					PMO_CommonErrorLog.error("Wykryto InterruptedException");
					return false;
				}
			}
		}
		return true;
	}

	public static boolean wait(CyclicBarrier cb) {
		if (cb != null) {
			try {
				cb.await();
				return true;
			} catch (InterruptedException | BrokenBarrierException e) {
				PMO_CommonErrorLog.error("W trakcie await wykryto wyjatek " + e.getMessage());
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	public static boolean testIfTrueAndWait(AtomicBoolean o) {
		if (o != null) {
			if (o.get()) {
				return wait(o);
			}
		}
		return true;
	}

}
