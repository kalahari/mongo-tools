package mongodump

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/intents"
	"github.com/mongodb/mongo-tools/common/log"
	"os"
	"path/filepath"
	"strings"
)

// shouldSkipCollection returns true when a collection name is excluded
// by the mongodump options.
func (dump *MongoDump) shouldSkipCollection(colName string) bool {
	for _, excludedCollection := range dump.OutputOptions.ExcludedCollections {
		if colName == excludedCollection {
			return true
		}
	}
	for _, excludedCollectionPrefix := range dump.OutputOptions.ExcludedCollectionPrefixes {
		if strings.HasPrefix(colName, excludedCollectionPrefix) {
			return true
		}
	}
	return false
}

// outputPath creates a path for the collection to be written to (sans file extension).
func (dump *MongoDump) outputPath(dbName, colName string) string {
	return filepath.Join(dump.OutputOptions.Out, dbName, colName)
}

// CreateIntentsForCollection builds an intent for a given collection and
// puts it into the intent manager.
func (dump *MongoDump) CreateIntentForCollection(dbName, colName string) error {
	if dump.shouldSkipCollection(colName) {
		log.Logf(log.DebugLow, "skipping dump of %v.%v, it is excluded", dbName, colName)
		return nil
	}

	intent := &intents.Intent{
		DB:           dbName,
		C:            colName,
		BSONPath:     dump.outputPath(dbName, colName) + ".bson",
		MetadataPath: dump.outputPath(dbName, colName) + ".metadata.json",
	}

	// add stdout flags if we're using stdout
	if dump.useStdout {
		intent.BSONPath = "-"
		intent.MetadataPath = "-"
	}

	// get a document count for scheduling purposes
	session, err := dump.sessionProvider.GetSession()
	if err != nil {
		return err
	}
	defer session.Close()

	count, err := session.DB(dbName).C(colName).Count()
	if err != nil {
		return fmt.Errorf("error counting %v: %v", intent.Namespace(), err)
	}
	intent.Size = int64(count)
	dump.manager.Put(intent)

	log.Logf(log.DebugLow, "enqueued collection '%v'", intent.Namespace())

	return nil
}

// CreateIntentsForDatabase iterates through collections in a db
// and builds dump intents for each collection.
func (dump *MongoDump) CreateIntentsForDatabase(dbName string) error {
	// we must ensure folders for empty databases are still created, for legacy purposes
	dbFolder := filepath.Join(dump.OutputOptions.Out, dbName)
	if !dump.OutputOptions.Tar {
		err := os.MkdirAll(dbFolder, defaultPermissions)
		if err != nil {
			return fmt.Errorf("error creating directory `%v`: %v", dbFolder, err)
		}
	}

	cols, err := dump.sessionProvider.CollectionNames(dbName)
	if err != nil {
		return fmt.Errorf("error getting collection names for database `%v`: %v", dbName, err)
	}

	log.Logf(log.DebugHigh, "found collections: %v", strings.Join(cols, ", "))
	// tar needs system collections to be first
	for _, colName := range cols {
		if strings.HasPrefix(colName, "system.") {
			if err = dump.CreateIntentForCollection(dbName, colName); err != nil {
				return err // no context needed
			}
		}
	}
	for _, colName := range cols {
		if !strings.HasPrefix(colName, "system.") {
			if err = dump.CreateIntentForCollection(dbName, colName); err != nil {
				return err // no context needed
			}
		}
	}
	return nil
}

// CreateAllIntents iterates through all dbs and collections and builds
// dump intents for each collection.
func (dump *MongoDump) CreateAllIntents() error {
	dbs, err := dump.sessionProvider.DatabaseNames()
	if err != nil {
		return fmt.Errorf("error getting database names: %v", err)
	}
	log.Logf(log.DebugHigh, "found databases: %v", strings.Join(dbs, ", "))
	for _, dbName := range dbs {
		if dbName == "local" {
			// local can only be explicitly dumped
			continue
		}
		if err := dump.CreateIntentsForDatabase(dbName); err != nil {
			return err
		}
	}
	return nil
}
