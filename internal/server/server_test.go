package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/hafizmfadli/proglog/api/v1"
	"github.com/hafizmfadli/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestServer defines our list of test cases and then runs a subtest for each case.
func TestServer(t *testing.T){
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeeds":
			testProduceConsume,
		"produce/consume stream succeeds":
			testProduceConsumeStream,
		"consume past log boundary fails":
			testConsumePastBoundary,
	}{
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

// setupTest is a helper function to set up each test case.
func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
){
	t.Helper()

	// creating listener on the local network address that our server will run on.
	// The 0 port is useful for when we don't care what port we use since 0 will
	// automatically assign us a free port.
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	// We then make an insecure connection to our listener and, with it, a client
	// we'll use to hit our server with.
	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}

	if fn != nil {
		fn(cfg)
	}

	// Create our server and start serving request in a goroutine because
	// the Serve method is a blocking call, and if we didn't run it in a goroutine
	// our test down would never run.
	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func(){
		server.Serve(l)
	}()

	client = api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}
}

// testProduceConsume test that producing and consuming works by using our client
// and server to produce a record to the log, consume it back, and then the record
// we sent is the same one we got back.
func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{
		Value: []byte("hello world"),
	}
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

// testConsumePastBoundary test that our sever responds with an api.ErrOffsetOutOfRange()
// error when a client tries to consume beyond the the log's boundaries
func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := grpc.Code(err)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

// testProduceConsumeStream is the streaming couterpart to testProduceConsume, testing
// that we can produce and consume through streams.
func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{
			Value: []byte("first message"),
			Offset: 0,
		},
		{
			Value: []byte("second message"),
			Offset: 1,
		},
	}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Offset,
					offset,
				)
			}
		}
	}
	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value: record.Value,
				Offset: uint64(i),
			})
		}
	}
}