package sizable

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/context"
)

type thing struct {
	ID     primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	Number uint               `bson:"number" json:"number"`
	User   string             `bson:"user" json:"user"`
}

var (
	db         *mongo.Database
	ctx        context.Context
	nineThings = []thing{
		{Number: 1, User: "jeff"},
		{Number: 2, User: "jeff"},
		{Number: 3, User: "jeff"},
		{Number: 4, User: "jeff"},
		{Number: 5, User: "tammy"},
		{Number: 6, User: "tammy"},
		{Number: 7, User: "tammy"},
		{Number: 8, User: "tammy"},
		{Number: 9, User: "tammy"},
	}
)

func TestMain(m *testing.M) {
	err := setup(m)
	if err != nil {
		panic(err)
	}
}

func TestDeleteAllWithCondition(t *testing.T) {
	assert.Nil(t, db.Drop(ctx))
	cllctn := db.Collection("things")

	var (
		err error
		res *mongo.DeleteResult
	)
	for _, thg := range nineThings {
		_, err = ReplaceOneUpsert(ctx, cllctn, bson.D{{"number", thg.Number}}, &thg)
		assert.Nil(t, err)
	}

	res, err = cllctn.DeleteMany(ctx, bson.D{{"user", "jeff"}})
	assert.Nil(t, err)
	assert.Equal(t, int64(4), res.DeletedCount)
}

func TestGetNFromCursor(t *testing.T) {
	assert.Nil(t, db.Drop(ctx))
	cllctn := db.Collection("things")

	var err error
	for _, thg := range nineThings {
		_, err = ReplaceOneUpsert(ctx, cllctn, bson.D{{"number", thg.Number}}, &thg)
		assert.Nil(t, err)
	}

	// First page
	crsr, err := cllctn.Find(ctx, bson.D{})
	assert.Nil(t, err)
	defer crsr.Close(ctx)

	var thgs []*thing
	assert.Nil(t, GetNFromCursor(ctx, crsr, 6, 0, &thgs))

	assert.Equal(t, 6, len(thgs))
	assert.Equal(t, thgs[len(thgs)-1].Number, nineThings[5].Number)
	assert.Equal(t, thgs[0].Number, nineThings[0].Number)

	// Second page
	crsr, err = cllctn.Find(ctx, bson.D{})
	assert.Nil(t, err)
	defer crsr.Close(ctx)

	var lastThreeThgs []*thing
	assert.Nil(t, GetNFromCursor(ctx, crsr, 6, 1, &lastThreeThgs))
	assert.Equal(t, 3, len(lastThreeThgs))
	assert.Equal(t, lastThreeThgs[0].Number, nineThings[6].Number)
	assert.Equal(t, lastThreeThgs[len(lastThreeThgs)-1].Number, nineThings[len(nineThings)-1].Number)

	assert.Equal(t, thgs[len(thgs)-1].Number, lastThreeThgs[0].Number-1)

	// Third page
	crsr, err = cllctn.Find(ctx, bson.D{})
	assert.Nil(t, err)
	defer crsr.Close(ctx)

	var emptyPage []*thing
	assert.Nil(t, GetNFromCursor(ctx, crsr, 6, 2, &emptyPage))
	assert.Equal(t, 0, len(emptyPage))
}

func setup(m *testing.M) error {
	var (
		err    error
		cancel context.CancelFunc
		clnt   *mongo.Client
	)

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	clnt, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		return err
	}
	db = clnt.Database("testing_sizable")
	err = db.Drop(ctx)
	if err != nil {
		return err
	}

	code := m.Run()

	err = clnt.Disconnect(ctx)
	if err != nil {
		return err
	}
	os.Exit(code)
	return nil
}
