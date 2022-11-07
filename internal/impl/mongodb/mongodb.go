package mongodb

import "github.com/benthosdev/benthos/v4/internal/impl/mongodb/client"

func isDocumentAllowed(op client.Operation) bool {
	switch op {
	case client.OperationInsertOne, client.OperationReplaceOne, client.OperationUpdateOne, client.OperationFindAndUpdate:
		return true
	default:
		return false
	}
}

func isFilterAllowed(op client.Operation) bool {
	switch op {
	case client.OperationDeleteOne, client.OperationDeleteMany, client.OperationReplaceOne, client.OperationUpdateOne, client.OperationFindOne, client.OperationFindAndUpdate:
		return true
	default:
		return false
	}
}

func isHintAllowed(op client.Operation) bool {
	switch op {
	case client.OperationDeleteOne, client.OperationDeleteMany, client.OperationReplaceOne, client.OperationUpdateOne, client.OperationFindOne, client.OperationFindAndUpdate:
		return true
	default:
		return false
	}
}

func isSortAllowed(op client.Operation) bool {
	switch op {
	case client.OperationFindOne, client.OperationFindAndUpdate:
		return true
	default:
		return false
	}
}

func isCommentAllowed(op client.Operation) bool {
	switch op {
	case client.OperationFindOne:
		return true
	default:
		return false
	}
}

func isUpsertAllowed(op client.Operation) bool {
	switch op {
	case client.OperationReplaceOne, client.OperationUpdateOne, client.OperationFindAndUpdate:
		return true
	default:
		return false
	}
}
