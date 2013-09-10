import org.freecompany.redline.Builder
import org.freecompany.redline.header.Architecture
import org.freecompany.redline.header.Os
import org.freecompany.redline.header.RpmType
import org.freecompany.redline.payload.Directive
import tablesaw.AbstractFileSet
import tablesaw.RegExFileSet
import tablesaw.Tablesaw
import tablesaw.TablesawException
import tablesaw.addons.GZipRule
import tablesaw.addons.TarRule
import tablesaw.addons.ivy.IvyAddon
import tablesaw.addons.java.Classpath
import tablesaw.addons.java.JavaCRule
import tablesaw.addons.java.JavaProgram
import tablesaw.addons.junit.JUnitRule
import tablesaw.rules.DirectoryRule
import tablesaw.rules.Rule
import tablesaw.rules.SimpleRule

import javax.swing.*

println("===============================================");

saw.setProperty(Tablesaw.PROP_MULTI_THREAD_OUTPUT, Tablesaw.PROP_VALUE_ON)

programName = "kairosdb"
//Do not use '-' in version string, it breaks rpm uninstall.
version = "0.9.1"
release = "3" //package release number
summary = "KairosDB"
description = """\
KairosDB is a time series database that stores numeric values along
with key/value tags to a nosql data store.  Currently supported
backends are Cassandra and HBase.  An H2 implementation is provided
for development work.
KairosDB is a rewrite of OpenTSDB to support modular interfaces.
"""
versionSourceDir = "build/src/org/kairosdb"
versionSource = "$versionSourceDir/KairosVersion.java"

saw = Tablesaw.getCurrentTablesaw()
saw.includeDefinitionFile("definitions.xml")

ivyConfig = ["default", "integration"]


rpmDir = "build/rpm"
rpmNoDepDir = "build/rpm-nodep"
new DirectoryRule("build")
rpmDirRule = new DirectoryRule(rpmDir)
rpmNoDepDirRule = new DirectoryRule(rpmNoDepDir)

ivy = new IvyAddon().setup()

buildLibraries = new RegExFileSet("lib", ".*\\.jar").recurse()
		.addExcludeDir("integration")
		.getFullFilePaths()

jp = new JavaProgram().setProgramName(programName)
		.setLibraryJars(buildLibraries)
		.setup()

jp.getCompileRule().getDefinition().set("target", "1.6")
jp.getCompileRule().getDefinition().set("source", "1.6")

additionalFiles = new RegExFileSet("src/main/java", ".*\\.sql").recurse()
jp.getJarRule().addFileSet(additionalFiles)
jp.getJarRule().addFiles("src/main/resources", "kairosdb.properties",
		"logback.xml")

//Set information in the manifest file
manifest = jp.getJarRule().getManifest().getMainAttributes()
manifest.putValue("Manifest-Version", "1.0")
manifest.putValue("Tablesaw-Version", saw.getVersion())
manifest.putValue("Created-By", saw.getProperty("java.vm.version")+" ("+
			saw.getProperty("java.vm.vendor")+")")
manifest.putValue("Built-By", saw.getProperty("user.name"))
buildDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")
manifest.putValue("Build-Date", buildDateFormat.format(new Date()))

buildNumberFormat = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
buildNumber = buildNumberFormat.format(new Date())
manifest.putValue("Implementation-Title", "KairosDB")
manifest.putValue("Implementation-Vendor", "Proofpoint Inc.")
manifest.putValue("Implementation-Version", "${version}.${buildNumber}")

//Add git revision information
gitRevisionFile= ".gitrevision"
new File(gitRevisionFile).text = ""
saw.exec(null, "git rev-parse HEAD", false, null, gitRevisionFile);
revision = new File(gitRevisionFile).text.trim()
new File(gitRevisionFile).delete()
manifest.putValue("Git-Revision", revision);

saw.setDefaultTarget("jar")


testClasspath = new Classpath(jp.getLibraryJars())
testClasspath.addPath(jp.getJarRule().getTarget())


testSources = new RegExFileSet("src/test/java", ".*Test\\.java").recurse()
		.addExcludeFiles("CassandraDatastoreTest.java", "HBaseDatastoreTest.java")
		.getFilePaths()
testCompileRule = jp.getTestCompileRule()

junitClasspath = new Classpath(testCompileRule.getClasspath())
junitClasspath.addPaths(testClasspath)
junitClasspath.addPath("src/main/java")
junitClasspath.addPath("src/test/resources")
junitClasspath.addPath("src/main/resources")

junit = new JUnitRule().addSources(testSources)
		.setClasspath(junitClasspath)
		.addDepends(testCompileRule)

if (saw.getProperty("jacoco", "false").equals("true"))
	junit.addJvmArgument("-javaagent:lib_test/jacocoagent.jar=destfile=build/jacoco.exec")

testSourcesAll = new RegExFileSet("src/test/java", ".*Test\\.java").recurse().getFilePaths()
junitAll = new JUnitRule("junit-test-all").setDescription("Run unit tests including Cassandra and HBase tests")
		.addSources(testSourcesAll)
		.setClasspath(junitClasspath)
		.addDepends(testCompileRule)

if (saw.getProperty("jacoco", "false").equals("true"))
	junitAll.addJvmArgument("-javaagent:lib_test/jacocoagent.jar=destfile=build/jacoco.exec")

//------------------------------------------------------------------------------
//Build zip deployable application
rpmFile = "$programName-$version-${release}.rpm"
srcRpmFile = "$programName-$version-${release}.src.rpm"
libFileSets = [
		new RegExFileSet("build/jar", ".*\\.jar"),
		new RegExFileSet("lib", ".*\\.jar"),
		new RegExFileSet("lib/ivy/default", ".*\\.jar")
	]

scriptsFileSet = new RegExFileSet("src/scripts", ".*").addExcludeFile("kairosdb-env.sh")
webrootFileSet = new RegExFileSet("webroot", ".*").recurse()

zipLibDir = "$programName/lib"
zipBinDir = "$programName/bin"
zipConfDir = "$programName/conf"
zipWebRootDir = "$programName/webroot"
tarRule = new TarRule("build/${programName}-${version}.tar")
		.addDepend(jp.getJarRule())
		.addFileSetTo(zipBinDir, scriptsFileSet)
		.addFileSetTo(zipWebRootDir, webrootFileSet)
		.addFileTo(zipConfDir, "src/main/resources", "kairosdb.properties")
		.setFilePermission(".*\\.sh", 0755)

for (AbstractFileSet fs in libFileSets)
	tarRule.addFileSetTo(zipLibDir, fs)


gzipRule = new GZipRule("package").setSource(tarRule.getTarget())
		.setDescription("Create deployable tar file")
		.setTarget("build/${programName}-${version}.tar.gz")
		.addDepend(tarRule)

//------------------------------------------------------------------------------
//Build rpm file
rpmBaseInstallDir = "/opt/$programName"
rpmRule = new SimpleRule("package-rpm").setDescription("Build RPM Package")
		.addDepend(jp.getJarRule())
		.addDepend(rpmDirRule)
		.addTarget("$rpmDir/$rpmFile")
		.setMakeAction("doRPM")
		.setProperty("dependency", "on")

new SimpleRule("package-rpm-nodep").setDescription("Build RPM Package with no dependencies")
		.addDepend(jp.getJarRule())
		.addDepend(rpmNoDepDirRule)
		.addTarget("${rpmNoDepDir}/$rpmFile")
		.setMakeAction("doRPM")

def doRPM(Rule rule)
{
	//Build rpm using redline rpm library
	host = InetAddress.getLocalHost().getHostName()
	rpmBuilder = new Builder()
	rpmBuilder.with
			{
				description = description
				group = "System Environment/Daemons"
				license = "license"
				setPackage(programName, version, release)
				setPlatform(Architecture.NOARCH, Os.LINUX)
				summary = summary
				type = RpmType.BINARY
				url = "http://code.google.com/p/kairosdb/"
				vendor = "Proofpoint Inc."
				provides = programName
				//prefixes = rpmBaseInstallDir
				buildHost = host
				sourceRpm = srcRpmFile
			}

	if ("on".equals(rule.getProperty("dependency")))
		rpmBuilder.addDependencyMore("jre", "1.6")

	rpmBuilder.setPostInstallScript(new File("src/scripts/install/post_install.sh"))
	rpmBuilder.setPreUninstallScript(new File("src/scripts/install/pre_uninstall.sh"))

	for (AbstractFileSet fs in libFileSets)
		addFileSetToRPM(rpmBuilder, "$rpmBaseInstallDir/lib", fs)

	addFileSetToRPM(rpmBuilder, "$rpmBaseInstallDir/bin", scriptsFileSet)

	rpmBuilder.addFile("/etc/init.d/kairosdb", new File("src/scripts/kairosdb-service.sh"), 0755)
	rpmBuilder.addFile("$rpmBaseInstallDir/conf/kairosdb.properties",
			new File("src/main/resources/kairosdb.properties"), 0644, new Directive(Directive.RPMFILE_CONFIG | Directive.RPMFILE_NOREPLACE))
	rpmBuilder.addFile("$rpmBaseInstallDir/bin/kairosdb-env.sh",
			new File("src/scripts/kairosdb-env.sh"), 0755, new Directive(Directive.RPMFILE_CONFIG | Directive.RPMFILE_NOREPLACE))

	for (AbstractFileSet.File f : webrootFileSet.getFiles())
		rpmBuilder.addFile("$rpmBaseInstallDir/webroot/$f.file", new File(f.getBaseDir(), f.getFile()))

	println("Building RPM "+rule.getTarget())
	outputFile = new FileOutputStream(rule.getTarget())
	rpmBuilder.build(outputFile.channel)
	outputFile.close()
}

def addFileSetToRPM(Builder builder, String destination, AbstractFileSet files)
{
	for (String filePath : files.getFullFilePaths())
	{
		File f = new File(filePath)
		if (f.getName().endsWith(".sh"))
			builder.addFile(destination + "/" +f.getName(), f, 0755)
		else
			builder.addFile(destination + "/" + f.getName(), f)
	}
}

debRule = new SimpleRule("package-deb").setDescription("Build Deb Package")
		.addDepend(rpmRule)
		.setMakeAction("doDeb")

def doDeb(Rule rule)
{
	//Prompt the user for the sudo password
	def jpf = new JPasswordField()
	def resp = JOptionPane.showConfirmDialog(null,
			jpf, "Enter sudo password:",
			JOptionPane.OK_CANCEL_OPTION)

	if (resp == 0)
	{
		def password = jpf.getPassword()
		sudo = saw.createAsyncProcess(rpmDir, "sudo -S alien --bump=0 --to-deb $rpmFile")
		sudo.run()
		//pass the password to the process on stdin
		sudo.sendMessage("$password\n")
		sudo.waitForProcess()
		if (sudo.getExitCode() != 0)
			throw new TablesawException("Unable to run alien application")
	}
}


//------------------------------------------------------------------------------
//Run the Kairos application
new SimpleRule("run-debug").setDescription("Runs kairosdb so a debugger can attach to port 5005")
		.addDepends(jp.getJarRule())
		.setMakeAction("doRun")
		.setProperty("DEBUG", true)

new SimpleRule("run").setDescription("Runs kairosdb")
		.addDepends(jp.getJarRule())
		.setMakeAction("doRun")
		.setProperty("DEBUG", false)

def doRun(Rule rule)
{
	//args = "-c import -f export.txt"
	//args = "-c export -f test_export.txt"
	args = "-c run"
	//Check if you have a custom kairosdb.properties file and load it.
	customProps = new File("kairosdb.properties")
	if (customProps.exists())
		args += " -p kairosdb.properties"

	debug = ""
	if (rule.getProperty("DEBUG"))
		debug = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"

	saw.exec("java ${debug} -Dio.netty.epollBugWorkaround=true -cp ${testClasspath} org.kairosdb.core.Main ${args}")
}


//------------------------------------------------------------------------------
//Generate GenORM Files for H2 module
genormDefinition = saw.getDefinition("genormous")
genormDefinition.set("genorm")
new SimpleRule("genorm").setDescription("Generate ORM files")
		.setMakeAction("doGenorm")

def doGenorm(Rule rule)
{
	genormClasspath = new Classpath(jp.getLibraryJars())
	genormDefinition.set("classpath", genormClasspath.toString())
	genormDefinition.set("source", "src/main/conf/tables.xml");
	cmd = genormDefinition.getCommand();
	saw.exec(cmd);
}


//------------------------------------------------------------------------------
//Build Integration tests
integrationClassPath = new Classpath(jp.getLibraryJars())
		.addPaths(new RegExFileSet("lib/ivy/integration", ".*\\.jar").getFullFilePaths())
		.addPath("src/integration-test/resources")

integrationBuildRule = new JavaCRule("build/integration")
		.addSourceDir("src/integration-test/java")
		.addClasspath(integrationClassPath)

new SimpleRule("integration")
		.setMakeAction("doIntegration")
		.addDepend(integrationBuildRule)

def doIntegration(Rule rule)
{
	host = saw.getProperty("host", "127.0.0.1")
	port = saw.getProperty("port", "8080")
	saw.exec("java  -Dhost=${host} -Dport=${port} -cp ${integrationBuildRule.classpath} org.testng.TestNG src/integration-test/testng.xml")
}



