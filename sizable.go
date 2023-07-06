package sizable

import (
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/context"
)

var (
	True = true
)

func handleSingleResult[T any](sRslt *mongo.SingleResult, ent *T) error {
	var err error
	err = sRslt.Err()
	if err != nil {
		return err
	}
	err = sRslt.Decode(ent)
	if err != nil {
		return err
	}
	return nil
}

func FindOneAndReplaceUpsert[T any](ctx context.Context, cllctn *mongo.Collection, fltr bson.D, ent *T) (*T, error) {
	var (
		err  error
		opts *options.FindOneAndReplaceOptions
		rslt *mongo.SingleResult
		aftr = options.After
	)

	opts = &options.FindOneAndReplaceOptions{Upsert: &True, ReturnDocument: &aftr}
	rslt = cllctn.FindOneAndReplace(ctx, fltr, ent, opts)

	err = handleSingleResult(rslt, ent)
	if err != nil {
		return nil, err
	}

	return ent, nil
}

func ReplaceOneUpsert[T any](ctx context.Context, cllctn *mongo.Collection, fltr bson.D, ent *T) (*mongo.UpdateResult, error) {
	opts := options.Replace().SetUpsert(true)
	return cllctn.ReplaceOne(ctx, fltr, ent, opts)
}

func GetNFromCursor[T any](ctx context.Context, crsr *mongo.Cursor, n int64, ents []*T) ([]*T, error) {
	var (
		ix  int64
		err error
	)
	defer crsr.Close(ctx)
	for ix = 0; ix < n; ix += 1 {
		var ent T
		if !crsr.Next(ctx) {
			return ents, nil
		}
		err = crsr.Decode(&ent)
		if err != nil {
			return ents, err
		}
		ents = append(ents, &ent)
	}
	return ents, nil
}

func RetrieveN[T any](ctx context.Context, cllctn *mongo.Collection, n int64, sort bson.D) ([]*T, error) {
	opts := options.Find().SetSort(sort)
	cursor, err := cllctn.Find(ctx, bson.D{}, opts)
	if err != nil {
		return nil, err
	}
	var ents []*T
	ents, err = GetNFromCursor(ctx, cursor, n, ents)
	if err != nil {
		return nil, err
	}

	return ents, nil
}

func InsertOne[T any](ctx context.Context, cllctn *mongo.Collection, ent *T) (primitive.ObjectID, error) {
	var (
		err        error
		ok         bool
		insOneRslt *mongo.InsertOneResult
		insOneId   primitive.ObjectID
	)
	insOneId = primitive.ObjectID{}

	insOneRslt, err = cllctn.InsertOne(ctx, ent)
	if err != nil {
		return insOneId, err
	}

	if insOneId, ok = insOneRslt.InsertedID.(primitive.ObjectID); !ok {
		return insOneId, errors.New(fmt.Sprintf("could not get object id from inserted id, result: %v", insOneRslt))
	}

	return insOneId, nil
}

func GetOne[T any](ctx context.Context, cllctn *mongo.Collection, fltr bson.D, ent *T) (*T, error) {
	var (
		err         error
		getByIdRslt *mongo.SingleResult
	)

	getByIdRslt = cllctn.FindOne(ctx, fltr)
	err = handleSingleResult[T](getByIdRslt, ent)
	if err != nil {
		fmt.Println("err getting result")
		return nil, err
	}

	return ent, nil
}

func FindByIds[T any](ctx context.Context, cllctn *mongo.Collection, ids []primitive.ObjectID, all []T) ([]T, error) {
	var (
		err          error
		fndByIdsFltr bson.D
	)

	fndByIdsFltr = bson.D{{"_id", bson.D{{"$in", ids}}}}
	cursor, err := cllctn.Find(ctx, fndByIdsFltr)
	if err != nil {
		return all, err
	}

	if err = cursor.All(ctx, &all); err != nil {
		return all, err
	}

	return all, nil
}

func Find[T any](ctx context.Context, cllctn *mongo.Collection, fltr bson.D, all []T) ([]T, error) {
	var (
		err error
	)

	cursor, err := cllctn.Find(ctx, fltr)
	if err != nil {
		return all, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &all); err != nil {
		return all, err
	}

	return all, nil
}
