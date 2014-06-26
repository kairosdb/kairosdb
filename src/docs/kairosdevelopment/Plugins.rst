=======
Plugins
=======

Plugins provide a way to extend Kairos in various ways.  With plugins you can:
  #. Add data point listeners
  #. Add a new data store to put data points into
  #. Add a new protocol handler
  #. Add a custom system monitor

For a good example of a simple plugin, see the [kairos-announce https://github.com/proofpoint/kairos-announce] project.  

-------------------
Plugin Requirements
-------------------
To create a plugin you need to do the following things
  #. Create a class that implements com.google.inject.AbstractModule.
  #. Create a properties file that has an entry kairosdb.service.[your_service_name] that is set to the full class name of your Module.
  #. Install a jar containing your class files to /opt/kairosdb/lib and install the properties file into /opt/kairosdb/conf

----------------------
Plugin Loading Process
----------------------
Here is how KairosDB loads libraries and plugins:
  #. All jar files in lib are automatically added to the classpath by the startup script.
  #. Properties in kairosdb.properties are loaded first from the classpath and then from the file.
  #. For every .properties file in the conf directory besides kairosdb.properties, the following happens:

    #. Kairos attempts to load the file from the classpath.  (this lets you add default values to future releases of your plugin)
    #. Kairos loads the conf properties file.

  #. Kairos looks through all the loaded properties for ones starting with kairosdb.service.  These are expected to point to Guice modules.
  #. Load all discovered Guice modules.
  #. Look through Guice bindings for implementations of KairosDBService and start them.

There is in essence only one Properties object in Kairos so, plugins can overwrite properties set in kairosdb.properties with their own.

For clarity lets look at an example of how things get loaded.  Say I create a new plugin called xplugin, my install places two files, xpluing.jar in /opt/kairosdb/lib and xplugin.properties in /opt/kairosdb/conf.

When KairosDB starts it first loads kairosdb.properties and then scans the conf directory for other .properties files.  KairosDB sees xplugin.properties and tries to find the file on the classpath.  This lets me as a plugin developer create a xplugin.properties that placed within my xplugin.jar file and contains default settings.  After loading the properties file on the classpath KairosDB loads the file from the conf directory.
