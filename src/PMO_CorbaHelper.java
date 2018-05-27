import org.omg.CORBA.ORB;
import org.omg.CosNaming.NameComponent;
import org.omg.CosNaming.NamingContext;
import org.omg.CosNaming.NamingContextHelper;
import org.omg.PortableServer.POA;

public class PMO_CorbaHelper {
	private POA rootPOA ;
	private ORB orb;
	
	public PMO_CorbaHelper( String[] args ) {
		try {
			orb = ORB.init(args, null);
			rootPOA = (POA) orb.resolve_initial_references("RootPOA");
			rootPOA.the_POAManager().activate();			
		} catch ( Exception e ) {
			PMO_CommonErrorLog.error( "Nie udalo sie polaczyc z orb");
			PMO_CommonErrorLog.criticalMistake();
		}
	}

	public POA getRootPOA() {
		return rootPOA;
	}
	
	public void rebind( String name, org.omg.CORBA.Object ref ) {
		try {
			org.omg.CORBA.Object namingContextObj = orb
					.resolve_initial_references("NameService");
			NamingContext nCont = NamingContextHelper.narrow(namingContextObj);
			NameComponent[] path = { new NameComponent(name, "Object") };
			
			nCont.rebind(path, ref);
		} catch ( Exception e ) {
			PMO_CommonErrorLog.error( "Nie udalo sie zarejstrowac uslugi");
			PMO_CommonErrorLog.criticalMistake();
		}
	}
	
}
