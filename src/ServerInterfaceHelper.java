
/**
* ServerInterfaceHelper.java .
* Generated by the IDL-to-Java compiler (portable), version "3.2"
* from interface.idl
* sobota, 2 września 2017 00:41:10 CEST
*/

abstract public class ServerInterfaceHelper
{
  private static String  _id = "IDL:ServerInterface:1.0";

  public static void insert (org.omg.CORBA.Any a, ServerInterface that)
  {
    org.omg.CORBA.portable.OutputStream out = a.create_output_stream ();
    a.type (type ());
    write (out, that);
    a.read_value (out.create_input_stream (), type ());
  }

  public static ServerInterface extract (org.omg.CORBA.Any a)
  {
    return read (a.create_input_stream ());
  }

  private static org.omg.CORBA.TypeCode __typeCode = null;
  synchronized public static org.omg.CORBA.TypeCode type ()
  {
    if (__typeCode == null)
    {
      __typeCode = org.omg.CORBA.ORB.init ().create_interface_tc (ServerInterfaceHelper.id (), "ServerInterface");
    }
    return __typeCode;
  }

  public static String id ()
  {
    return _id;
  }

  public static ServerInterface read (org.omg.CORBA.portable.InputStream istream)
  {
    return narrow (istream.read_Object (_ServerInterfaceStub.class));
  }

  public static void write (org.omg.CORBA.portable.OutputStream ostream, ServerInterface value)
  {
    ostream.write_Object ((org.omg.CORBA.Object) value);
  }

  public static ServerInterface narrow (org.omg.CORBA.Object obj)
  {
    if (obj == null)
      return null;
    else if (obj instanceof ServerInterface)
      return (ServerInterface)obj;
    else if (!obj._is_a (id ()))
      throw new org.omg.CORBA.BAD_PARAM ();
    else
    {
      org.omg.CORBA.portable.Delegate delegate = ((org.omg.CORBA.portable.ObjectImpl)obj)._get_delegate ();
      _ServerInterfaceStub stub = new _ServerInterfaceStub ();
      stub._set_delegate(delegate);
      return stub;
    }
  }

  public static ServerInterface unchecked_narrow (org.omg.CORBA.Object obj)
  {
    if (obj == null)
      return null;
    else if (obj instanceof ServerInterface)
      return (ServerInterface)obj;
    else
    {
      org.omg.CORBA.portable.Delegate delegate = ((org.omg.CORBA.portable.ObjectImpl)obj)._get_delegate ();
      _ServerInterfaceStub stub = new _ServerInterfaceStub ();
      stub._set_delegate(delegate);
      return stub;
    }
  }

}