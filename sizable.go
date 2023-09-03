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

func FindOneAndReplaceUpsert[T any](ctx context.Context, cllctn *mongo.Collection, fltr bson.D, ent *T) error {
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
		return err
	}

	return nil
}

func ReplaceOneUpsert[T any](ctx context.Context, cllctn *mongo.Collection, fltr bson.D, ent *T) (*mongo.UpdateResult, error) {
	opts := options.Replace().SetUpsert(true)
	return cllctn.ReplaceOne(ctx, fltr, ent, opts)
}

func GetNFromCursor[T any](ctx context.Context, crsr *mongo.Cursor, n int64, page int64, ents *[]*T) error {
	var (
		ix  int64
		err error
	)
	defer crsr.Close(ctx)

	for i := 0; i < int(page*n); i++ {
		if !crsr.Next(ctx) {
			return nil
		}
	}

	for ix = 0; ix < n; ix += 1 {
		var ent T
		if !crsr.Next(ctx) {
			return nil
		}
		err = crsr.Decode(&ent)
		if err != nil {
			return err
		}
		*ents = append(*ents, &ent)
	}
	return nil
}

func RetrieveN[T any](ctx context.Context, cllctn *mongo.Collection, n int64, sort bson.D) ([]*T, error) {
	opts := options.Find().SetSort(sort)
	cursor, err := cllctn.Find(ctx, bson.D{}, opts)
	if err != nil {
		return nil, err
	}
	var ents []*T
	err = GetNFromCursor(ctx, cursor, n, 0, &ents)
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

func GetOne[T any](ctx context.Context, cllctn *mongo.Collection, fltr bson.D, ent *T) error {
	var (
		err         error
		getByIdRslt *mongo.SingleResult
	)

	getByIdRslt = cllctn.FindOne(ctx, fltr)
	err = getByIdRslt.Err()
	if err != nil {
		return err
	}

	err = handleSingleResult[T](getByIdRslt, ent)
	return err
}

func DeleteOne(ctx context.Context, cllctn *mongo.Collection, fltr bson.D) error {
	var (
		err error
		res *mongo.DeleteResult
	)

	res, err = cllctn.DeleteOne(ctx, fltr)
	if err != nil {
		return err
	}

	if res.DeletedCount == 0 {
		return errors.New("Expected entities to be deleted, but none were.")
	}

	return nil
}

func FindByIds[T any](ctx context.Context, cllctn *mongo.Collection, ids []primitive.ObjectID, all *[]T) error {
	var (
		err          error
		fndByIdsFltr bson.D
	)

	fndByIdsFltr = bson.D{{"_id", bson.D{{"$in", ids}}}}
	cursor, err := cllctn.Find(ctx, fndByIdsFltr)
	if err != nil {
		return err
	}

	if err = cursor.All(ctx, all); err != nil {
		return err
	}

	return nil
}

func Find[T any](ctx context.Context, cllctn *mongo.Collection, fltr bson.D, all *[]T) error {
	var (
		err error
	)

	cursor, err := cllctn.Find(ctx, fltr)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, all); err != nil {
		return err
	}

	return nil
}
