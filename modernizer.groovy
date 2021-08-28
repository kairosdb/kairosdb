import tablesaw.*
import java.util.regex.Pattern
import org.gaul.modernizer_maven_plugin.Modernizer
import org.gaul.modernizer_maven_plugin.Violation
import org.gaul.modernizer_maven_plugin.ViolationOccurrence

InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("modernizer.xml");

Map<String, Violation> violationMap = Modernizer.parseFromXml(is);

Modernizer modernizer = new Modernizer("1.8", violationMap, new ArrayList<String>(), new ArrayList<Pattern>(),
		new ArrayList<String>(), new HashSet<String>(), new ArrayList<Pattern>())

buildDir = jc.getBuildDirectory()

classFiles = new RegExFileSet(buildDir, ".*\\.class").recurse().getFullFilePaths()

int violations = 0;
println classFiles.size()
for (String arg : classFiles)
{
	InputStream classIs = new FileInputStream(arg);
	Collection<ViolationOccurrence> check = modernizer.check(classIs);

	if (check.size() != 0)
	{
		println arg
		violations += check.size()

		for (ViolationOccurrence result : check)
		{
			println "   Line: "+result.getLineNumber()+" - "+result.getViolation().getComment()
		}
	}
}

println "Found "+violations+ " violations"