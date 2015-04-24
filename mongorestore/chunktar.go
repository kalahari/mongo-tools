package mongorestore

import (
	"github.com/mongodb/mongo-tools/common/intents"
)

type TarRestoreState struct {
	PastDbs           []string
	CurrentDb         string
	SingleDb          bool
	PastCollections   []string
	CurrentCollection string
	SingleCollection  bool
	UsesMetadataFiles bool
	RestoredMetadata  bool
	RestoredBSON      bool
	CollectionExists  bool
	MetadataIndexes   []IndexDocument
	SystemIndexes     map[string][]IndexDocument
	Intent            *intents.Intent
	restore           *MongoRestore
}

func (restore *MongoRestore) NewTarRestoreState() *TarRestoreState {
	return &TarRestoreState{
		PastDbs:           make([]string, 0, 10),
		CurrentDb:         restore.ToolOptions.DB,
		SingleDb:          restore.ToolOptions.DB != "",
		PastCollections:   make([]string, 0, 10),
		CurrentCollection: restore.ToolOptions.Collection,
		SingleCollection:  restore.ToolOptions.Collection != "",
		UsesMetadataFiles: false,
		RestoredMetadata:  false,
		RestoredBSON:      false,
		CollectionExists:  false,
		MetadataIndexes:   nil,
		SystemIndexes:     nil,
		Intent:            nil,
		restore:           restore,
	}
}

func (state *TarRestoreState) ChangeDatabase(db string) error {
	if state.CurrentDb != "" {
		state.PastDbs = append(state.PastDbs, state.CurrentDb)
	}
	state.CurrentDb = db
	state.PastCollections = make([]string, 0, 10)
	state.SystemIndexes = nil
	return state.ChangeCollection("")
}

func (state *TarRestoreState) ChangeCollection(collection string) error {
	if state.CurrentCollection != "" {
		state.PastCollections = append(state.PastCollections, state.CurrentCollection)
	}
	state.CurrentCollection = collection
	state.UsesMetadataFiles = false
	if state.RestoredMetadata {
		err := state.restore.RestoreCollectionIndexes(state.Intent, state.MetadataIndexes)
		if err != nil {
			return err
		}
	} else if state.SystemIndexes != nil && state.SystemIndexes[state.Intent.C] != nil {
		err := state.restore.RestoreCollectionIndexes(state.Intent, state.SystemIndexes[state.Intent.C])
		if err != nil {
			return err
		}
	}
	state.RestoredMetadata = false
	state.RestoredBSON = false
	state.CollectionExists = false
	state.MetadataIndexes = nil
	return nil
}
