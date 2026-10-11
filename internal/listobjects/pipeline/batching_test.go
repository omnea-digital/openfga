package pipeline_test

import (
	"context"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/openfga/openfga/internal/listobjects/pipeline"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

// batchingConfigs are the extremes of the batching knobs. Every one of them must
// return exactly what the default configuration returns.
var batchingConfigs = map[string]pipeline.Config{
	"chunk_1":                 {BufferCapacity: 128, ChunkSize: 1, NumProcs: 3},
	"chunk_5000":              {BufferCapacity: 128, ChunkSize: 5000, NumProcs: 3},
	"procs_1":                 {BufferCapacity: 128, ChunkSize: 100, NumProcs: 1},
	"procs_8":                 {BufferCapacity: 128, ChunkSize: 100, NumProcs: 8},
	"buffer_1":                {BufferCapacity: 1, ChunkSize: 1, NumProcs: 1},
	"chunk_1_max_batch_1000":  {BufferCapacity: 128, ChunkSize: 1, NumProcs: 3, MaxBatch: 1000},
	"chunk_100_max_batch_1k":  {BufferCapacity: 128, ChunkSize: 100, NumProcs: 3, MaxBatch: 1000},
	"chunk_7_max_batch_10":    {BufferCapacity: 2, ChunkSize: 7, NumProcs: 8, MaxBatch: 10},
	"chunk_1_procs_1_batch_2": {BufferCapacity: 128, ChunkSize: 1, NumProcs: 1, MaxBatch: 2},
}

func TestPipeline_BatchingConfigs(t *testing.T) {
	for configName, config := range batchingConfigs {
		for _, tc := range cases {
			t.Run(configName+"/"+tc.name, func(t *testing.T) {
				defer goleak.VerifyNone(t)

				model := testutils.MustTransformDSLToProtoWithID(tc.model)
				ds := NewTestStore()
				require.NoError(t, ds.WriteAuthorizationModel(context.Background(), Store, model))
				ds.Add(t, tc.tuples...)

				typesys, err := typesystem.NewAndValidate(context.Background(), model)
				require.NoError(t, err)

				reader := pipeline.NewValidatingStore(
					ds,
					Store,
					pipeline.WithStoreValidator(pipeline.NewValidator(context.Background(), typesys, nil)),
				)

				config.Flags = tc.flags
				builder, err := pipeline.NewBuilder(reader, pipeline.WithConfig(config))
				require.NoError(t, err)

				p, err := builder.Build(context.Background(), typesys.GetWeightedGraph(), pipeline.Spec{
					ObjectType:     tc.objectType,
					ObjectRelation: tc.relation,
					SubjectType:    tc.subjectType,
					SubjectID:      tc.subjectID,
				})
				if tc.error != nil {
					require.ErrorIs(t, err, tc.error)
					return
				}
				require.NoError(t, err)

				evaluate(t, tc, p)
			})
		}
	}
}

type countingStore struct {
	pipeline.ObjectStore
	reads atomic.Int64
}

func (s *countingStore) Read(ctx context.Context, q pipeline.ObjectQuery) pipeline.Receiver[pipeline.Item] {
	s.reads.Add(1)
	return s.ObjectStore.Read(ctx, q)
}

// lineageModel has the shape of a recursive lineage: a record is visible through its
// own grants, or through its parent's visibility, and a second recursive relation
// fans out over two parent relations.
const lineageModel = `
model
	schema 1.1

type user

type org
	relations
		define member: [user]

type folder
	relations
		define org: [org]
		define parent: [folder]
		define parent_shared: [folder]
		define owner: [user]
		define own_view: owner or member from org
		define lineage_view: own_view or lineage_view from parent or lineage_view from parent_shared
		define can_view: own_view or can_view from parent or lineage_view
`

// lineageWorld builds a random forest of folders and returns its tuples and the
// folders user:u can view, computed independently of the pipeline.
func lineageWorld(size int, seed int64) ([]string, []string) {
	rng := rand.New(rand.NewSource(seed))

	var tuples []string
	children := make(map[int][]int)
	seeds := make(map[int]bool)

	tuples = append(tuples, "org:o#member@user:u")
	for i := range size {
		id := "folder:" + strconv.Itoa(i)
		switch {
		case i > 0 && rng.Intn(3) != 0:
			parent := rng.Intn(i)
			rel := "parent"
			if rng.Intn(5) == 0 {
				rel = "parent_shared"
			}
			tuples = append(tuples, id+"#"+rel+"@folder:"+strconv.Itoa(parent))
			children[parent] = append(children[parent], i)
		default:
			if rng.Intn(4) == 0 {
				tuples = append(tuples, id+"#org@org:o")
				seeds[i] = true
			}
		}
		if rng.Intn(50) == 0 {
			tuples = append(tuples, id+"#owner@user:u")
			seeds[i] = true
		}
	}

	visible := make(map[int]bool)
	var stack []int
	for s := range seeds {
		stack = append(stack, s)
	}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visible[n] {
			continue
		}
		visible[n] = true
		stack = append(stack, children[n]...)
	}

	expected := make([]string, 0, len(visible))
	for n := range visible {
		expected = append(expected, "folder:"+strconv.Itoa(n))
	}
	return tuples, expected
}

func TestPipeline_BatchingLineageWorld(t *testing.T) {
	tuples, expected := lineageWorld(5000, 7)
	require.Greater(t, len(expected), 500)

	model := testutils.MustTransformDSLToProtoWithID(lineageModel)
	ds := NewTestStore()
	require.NoError(t, ds.WriteAuthorizationModel(context.Background(), Store, model))
	ds.Add(t, tuples...)

	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)
	graph := typesys.GetWeightedGraph()
	require.NotNil(t, graph)

	run := func(t *testing.T, config pipeline.Config) ([]string, int64) {
		t.Helper()
		store := &countingStore{ObjectStore: pipeline.NewValidatingStore(
			ds,
			Store,
			pipeline.WithStoreValidator(pipeline.NewValidator(context.Background(), typesys, nil)),
		)}
		builder, err := pipeline.NewBuilder(store, pipeline.WithConfig(config))
		require.NoError(t, err)
		p, err := builder.Build(context.Background(), graph, pipeline.Spec{
			ObjectType:     "folder",
			ObjectRelation: "can_view",
			SubjectType:    "user",
			SubjectID:      "u",
		})
		require.NoError(t, err)
		defer p.Close()

		var results []string
		for {
			value, ok := p.Recv(context.Background())
			if !ok {
				break
			}
			results = append(results, value)
		}
		require.NoError(t, p.Err())
		return results, store.reads.Load()
	}

	reads := make(map[string]int64)
	for name, config := range batchingConfigs {
		t.Run(name, func(t *testing.T) {
			defer goleak.VerifyNone(t)
			results, n := run(t, config)
			require.ElementsMatch(t, expected, results)
			reads[name] = n
			t.Logf("%s: %d results, %d reads", name, len(results), n)
		})
	}

	// Merging only ever removes reads: each merged read covers reads the
	// unmerged configuration would have issued one by one.
	require.LessOrEqual(t, reads["chunk_1_max_batch_1000"], reads["chunk_1"])
	require.Less(t, reads["chunk_5000"], reads["chunk_1"])
}
