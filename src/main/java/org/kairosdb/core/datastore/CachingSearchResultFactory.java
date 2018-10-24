package org.kairosdb.core.datastore;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.KairosPostConstructInit;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.google.common.base.Preconditions.checkState;

/**
 * Stores data store results in a file cache.
 */
public class CachingSearchResultFactory implements SearchResultFactory, KairosPostConstructInit {

    public static final String KEEP_CACHE_FILES = "kairosdb.query_cache.keep_cache_files";
    public static final String QUERY_CACHE_DIR = "kairosdb.query_cache.cache_dir";

    private static final Logger logger = LoggerFactory.getLogger(CachingSearchResultFactory.class);

    private File cacheBaseDir;
    private volatile File cacheDir;
    private final KairosDataPointFactory dataPointFactory;
    private final Datastore datastore;
    private final boolean keepCacheFiles;

    @Inject
    public CachingSearchResultFactory(KairosDataPointFactory dataPointFactory, Datastore datastore, @Named(KEEP_CACHE_FILES) boolean keepCacheFiles) {
        this.cacheBaseDir = new File(System.getProperty("java.io.tmpdir"),  "kairos_cache");
        this.dataPointFactory = dataPointFactory;
        this.datastore = datastore;
        this.keepCacheFiles = keepCacheFiles;
    }

    @Override
    public SearchResult createSearchResult(QueryMetric queryMetric, DatastoreQueryContext datastoreQueryContext) throws DatastoreException {
        String cacheFileName = Hashing.md5().hashString(queryMetric.getCacheString(), Charsets.UTF_8).toString();
        File cacheFileBaseName = new File(cacheDir, cacheFileName);

        SearchResult searchResult = null;
        try {
            if (queryMetric.getCacheTime() > 0) {
                searchResult = CachedSearchResult.openCachedSearchResult(queryMetric.getName(),
                        cacheFileBaseName.getAbsolutePath(), queryMetric.getCacheTime(), dataPointFactory, keepCacheFiles);
                if (searchResult != null) {
                    searchResult.getRows();
                    logger.debug("Cache HIT!");
                }
            }

            if (searchResult == null) {
                logger.debug("Cache MISS!");
                searchResult = CachedSearchResult.createCachedSearchResult(queryMetric.getName(),
                        cacheFileBaseName.getAbsolutePath(), dataPointFactory, keepCacheFiles);
                datastore.queryDatabase(queryMetric, searchResult);
            }
        } catch (Exception e) {
            logger.error("Query error", e);
            if (e instanceof DatastoreException) {
                throw (DatastoreException)e;
            }
            throw new DatastoreException(e);
        }

        return searchResult;
    }

    @SuppressWarnings("UnusedDeclaration")
    @Inject(optional = true)
    public void setBaseCacheDir(@Named(QUERY_CACHE_DIR) String cacheTempDir)
    {
        if (cacheTempDir != null && !cacheTempDir.equals(""))
        {
            cacheBaseDir = new File(cacheTempDir);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void setupCacheDirectory()
    {
        cleanDirectory(cacheBaseDir);
        newCacheDirectory();
        cacheDir.mkdirs();
        checkState(cacheDir.exists(), "Unable to create Cache directory '" + cacheDir + "'");
    }

    @Override
    public void init()
    {
        setupCacheDirectory();
    }



    /** Make sure the folder exists */
    private static void ensureDirectory(File directory)
    {
        if (!directory.exists())
            directory.mkdirs();
    }

    public File getCacheDir()
    {
        ensureDirectory(cacheDir);
        return (cacheDir);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void newCacheDirectory()
    {
        File newCacheDir = new File(cacheBaseDir, String.valueOf(System.currentTimeMillis()));
        ensureDirectory(newCacheDir);
        cacheDir = newCacheDir;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void cleanDirectory(File directory)
    {
        if (!directory.exists())
            return;
        File[] list = directory.listFiles();

        if (list != null && list.length > 0)
        {
            for (File aList : list)
            {
                if (aList.isDirectory())
                    cleanDirectory(aList);

                aList.delete();
            }
        }

        directory.delete();
    }

    public void cleanCacheDir(boolean wait)
    {
        File oldCacheDir = cacheDir;
        newCacheDirectory();

        if (wait)
        {
            try
            {
                Thread.sleep(60000);
            }
            catch (InterruptedException e)
            {
                logger.error("Sleep interrupted:", e);
            }
        }

        logger.debug("Executing job...");
        logger.debug("Deleting cache files in " + oldCacheDir.getAbsolutePath());

        cleanDirectory(oldCacheDir);
    }
}
