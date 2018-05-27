import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class PMO_Test implements PMO_RunTestTimeout {

	private AtomicBoolean finished = new AtomicBoolean(false);
	private List<PMO_User> users = new ArrayList<>();
	private ServerInterface server;
	private PMO_CorbaHelper corba;
	private PMO_AtomicCounter maxCPUUsage;

	public PMO_Test(ServerInterface server, PMO_CorbaHelper corba) {
		this.server = server;
		this.corba = corba;
	}

	protected boolean executeTests(List<? extends PMO_Testable> tests) {
		boolean result = true;
		boolean testResult;
		int errorCounter = PMO_Consts.THE_SAME_ERROR_REPETITIONS_LIMIT;

		for (PMO_Testable test : tests) {
			testResult = test.test();
			if (testResult == false)
				errorCounter--;
			result &= testResult;
			if (errorCounter == 0) {
				PMO_CommonErrorLog.error("Przekroczono limit prezentacji bledow - koniec testu");
				return false;
			}
		}
		return result;
	}

	@Override
	public long getRequiredTime() {
		return 20 * (PMO_Consts.TASK_SLEEP_TIME_MIN + PMO_Consts.TASK_SLEEP_TIME_DELTA);
	}

	protected void prepareUsers() {
		PMO_User u1 = new PMO_User("U3A", 3, server, corba, false, 12);
		PMO_User u2 = new PMO_User("U2", 2, server, corba, false, 12);
		PMO_User u3 = new PMO_User("U4", 4, server, corba, false, 12);
		PMO_User u4 = new PMO_User("U3B", 3, server, corba, false, 12);

		users.add(u1);
		users.add(u2);
		users.add(u3);
		users.add(u4);

		Set<Integer> uniqID = Collections.synchronizedSet(new TreeSet<>());
		users.forEach(u -> u.setGlobalUniqUserID(uniqID));

		maxCPUUsage = PMO_CountersFactory.createCommonMaxStorageCounter();

		PMO_AtomicCounter cpuUsage = PMO_CountersFactory.createCounterWithMaxStorageSet();

		users.forEach(u -> u.setCPUUsageCounter(cpuUsage));
	}

	@Override
	public void run() {

		PMO_Log.log("Przygotowanie uzytkownikow");
		prepareUsers();

		PMO_Log.log("Uruchomienie watkow");
		List<Thread> threads = PMO_ThreadsHelper.createAndStartThreads(users, true);
		PMO_Log.log("Oczekiwanie na zakonczenie watkow");
		PMO_ThreadsHelper.joinThreads(threads);
		PMO_Log.log("Watki uzytkownikow zakonczyly prace");

		finished.set(true);
	}

	@Override
	public boolean test() {

		if (!finished.get()) {
			PMO_CommonErrorLog.error("Rozpoczeto test, choc metoda run() jeszcze sie nie zakonczyla");
			return false;
		}

		if (maxCPUUsage.get() < PMO_Consts.CORES / 2) {
			PMO_CommonErrorLog.error("Wykryto niedostateczne uzycie CPU.");
			PMO_CommonErrorLog.error("Uzywano max " + maxCPUUsage.get() + " na " + PMO_Consts.CORES + " rdzeni");
			return false;
		} else {
			PMO_Log.log("Uzycie CPU. Uzyto " + maxCPUUsage.get() + " rdzeni");
		}

		boolean result = true;

		result &= executeTests(users);

		return result;
	}
}
