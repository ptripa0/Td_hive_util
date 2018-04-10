SELECT count(*) as DIFF 
FROM @fromdb.testapplication X 
LEFT OUTER JOIN @todb.testapplication Y ON 
X.id = Y.id AND X.app_version=Y.app_version AND X.database_version=Y.database_version AND X.last_updated = Y.last_updated AND X.installation_platform = Y.installation_platform 
WHERE Y.id IS NULL;
