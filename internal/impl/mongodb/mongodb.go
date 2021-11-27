package mongodb

import "github.com/Jeffail/benthos/v3/internal/impl/mongodb/client"

func isDocumentAllowed(op client.Operation) bool {
	switch op {
	case client.OperationInsertOne, client.OperationReplaceOne, client.OperationUpdateOne:
		return true
	default:
		return false
	}
}

func isFilterAllowed(op client.Operation) bool {
	switch op {
	case client.OperationDeleteOne, client.OperationDeleteMany, client.OperationReplaceOne, client.OperationUpdateOne, client.OperationFindOne:
		return true
	default:
		return false
	}
}

func isHintAllowed(op client.Operation) bool {
	switch op {
	case client.OperationDeleteOne, client.OperationDeleteMany, client.OperationReplaceOne, client.OperationUpdateOne, client.OperationFindOne:
		return true
	default:
		return false
	}
}

func isUpsertAllowed(op client.Operation) bool {
	switch op {
	case client.OperationReplaceOne, client.OperationUpdateOne:
		return true
	default:
		return false
	}
}
