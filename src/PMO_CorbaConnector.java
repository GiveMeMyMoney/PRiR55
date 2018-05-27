import org.omg.CORBA.ORB;
import org.omg.CosNaming.*;

import java.lang.reflect.Method;

public class PMO_CorbaConnector {

	private static void logAndDisplay(String txt) {
		PMO_Log.log(txt);
		PMO_SystemOutRedirect.print(txt);
	}

	/**
	 * Metoda laczy z serwisem CORBA o podanej nazwie.
	 * 
	 * @param argv
	 *            argumenty wywolania programu
	 * @param name
	 *            nazwa serwisu CORBA
	 * @param interfaceName
	 *            nazwa docelowego interfejsu. Do nazwy dodawany jest czlon
	 *            Helper
	 * @return obiekt (namiastka) reprezentujacy serwis
	 */
	public static Object connectToServer(String[] argv, String name, String interfaceName) {
		logAndDisplay("Polaczenie do serwera - poczatek");
		ORB orb = ORB.init(argv, null);
		org.omg.CORBA.Object namingContextObj;
		Object result;
		Class<?> classObject = null;
		Method narrowMethod = null;
		try {
			classObject = Class.forName(interfaceName + "Helper");
			narrowMethod = classObject.getMethod("narrow", org.omg.CORBA.Object.class);
		} catch (ClassNotFoundException e2) {
			e2.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}

		try {
			namingContextObj = orb.resolve_initial_references("NameService");
			NamingContext namingContext = NamingContextHelper.narrow(namingContextObj);

			NameComponent[] path = { new NameComponent(name, "Object") };

			org.omg.CORBA.Object envObj = namingContext.resolve(path);

			result = narrowMethod.invoke(null, envObj);

			logAndDisplay("Polaczenie do serwera - OK");
			return result;
		} catch (Exception e) {
			logAndDisplay("Odebrano wyjatek juz w trakcie polaczenia do systemu");
			logAndDisplay("Test polaczenia bezposredniego");
			try {
				namingContextObj = orb.resolve_initial_references("NameService");
				NamingContextExt ncRef = NamingContextExtHelper.narrow(namingContextObj);
				NameComponent path[] = ncRef.to_name(name);
				org.omg.CORBA.Object envObj = ncRef.resolve(path);
				result = narrowMethod.invoke(null, envObj);

				logAndDisplay("Polaczenie do serwera - OK");
				return result;
			} catch (Exception e1) {
				logAndDisplay("Podobnie bez skutku - nie mozna odszukac serwisu");
			}

		}
		logAndDisplay("Brak polacznia z serwisem - nie mozna kontynuowac testu");
		PMO_CommonErrorLog.error("Nie udalo sie nawiazac polaczenia z serwisem " + name);
		PMO_CommonErrorLog.criticalMistake();
		return null;
	}

}
