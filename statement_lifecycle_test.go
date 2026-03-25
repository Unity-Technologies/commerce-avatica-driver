package avatica

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/apache/calcite-avatica-go/v5/message"
	"google.golang.org/protobuf/proto"
)

const (
	testWireRequestPrefix  = "org.apache.calcite.avatica.proto.Requests$"
	testWireResponsePrefix = "org.apache.calcite.avatica.proto.Responses$"
)

type fakeAvaticaServer struct {
	t            *testing.T
	server       *httptest.Server
	mu           sync.Mutex
	nextStmtID   uint32
	stmtQueries  map[uint32]string
	prepareCount int
	closeCount   int
}

func newFakeAvaticaServer(t *testing.T) *fakeAvaticaServer {
	f := &fakeAvaticaServer{
		t:           t,
		nextStmtID:  1,
		stmtQueries: make(map[uint32]string),
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	return f
}

func (f *fakeAvaticaServer) close() {
	f.server.Close()
}

func (f *fakeAvaticaServer) statementCounts() (prepareCount int, closeCount int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.prepareCount, f.closeCount
}

func (f *fakeAvaticaServer) handle(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	payload, err := ioReadAll(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	wire := &message.WireMessage{}
	if err := proto.Unmarshal(payload, wire); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch wire.GetName() {
	case testWireRequestPrefix + "OpenConnectionRequest":
		f.writeResponse(w, "OpenConnectionResponse", message.OpenConnectionResponse_builder{}.Build())
	case testWireRequestPrefix + "DatabasePropertyRequest":
		f.writeResponse(w, "DatabasePropertyResponse", message.DatabasePropertyResponse_builder{
			Props: []*message.DatabasePropertyElement{
				message.DatabasePropertyElement_builder{
					Key: message.DatabaseProperty_builder{Name: "GET_DRIVER_NAME"}.Build(),
					Value: message.TypedValue_builder{
						Type:        message.Rep_STRING,
						StringValue: "Generic",
					}.Build(),
				}.Build(),
			},
		}.Build())
	case testWireRequestPrefix + "PrepareRequest":
		req := &message.PrepareRequest{}
		if err := proto.Unmarshal(wire.GetWrappedMessage(), req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		f.mu.Lock()
		stmtID := f.nextStmtID
		f.nextStmtID++
		f.stmtQueries[stmtID] = req.GetSql()
		f.prepareCount++
		f.mu.Unlock()

		f.writeResponse(w, "PrepareResponse", message.PrepareResponse_builder{
			Statement: message.StatementHandle_builder{
				ConnectionId: req.GetConnectionId(),
				Id:           stmtID,
				Signature: message.Signature_builder{
					Parameters: []*message.AvaticaParameter{
						message.AvaticaParameter_builder{TypeName: "BIGINT"}.Build(),
					},
				}.Build(),
			}.Build(),
		}.Build())
	case testWireRequestPrefix + "ExecuteRequest":
		req := &message.ExecuteRequest{}
		if err := proto.Unmarshal(wire.GetWrappedMessage(), req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		statementID := req.GetStatementHandle().GetId()
		query := f.queryForStatement(statementID)
		rows := []*message.Row{}
		if !strings.Contains(strings.ToLower(query), "missing") {
			rows = append(rows, message.Row_builder{
				Value: []*message.ColumnValue{
					message.ColumnValue_builder{
						ScalarValue: message.TypedValue_builder{
							Type:        message.Rep_LONG,
							NumberValue: 1,
						}.Build(),
					}.Build(),
				},
			}.Build())
		}

		signature := message.Signature_builder{
			Columns: []*message.ColumnMetaData{
				message.ColumnMetaData_builder{
					ColumnName: "v",
					Type: message.AvaticaType_builder{
						Name: "BIGINT",
						Rep:  message.Rep_LONG,
					}.Build(),
				}.Build(),
			},
		}.Build()

		f.writeResponse(w, "ExecuteResponse", message.ExecuteResponse_builder{
			Results: []*message.ResultSetResponse{
				message.ResultSetResponse_builder{
					StatementId: statementID,
					Signature:   signature,
					FirstFrame: message.Frame_builder{
						Done: true,
						Rows: rows,
					}.Build(),
				}.Build(),
			},
		}.Build())
	case testWireRequestPrefix + "CloseStatementRequest":
		req := &message.CloseStatementRequest{}
		if err := proto.Unmarshal(wire.GetWrappedMessage(), req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		f.mu.Lock()
		f.closeCount++
		delete(f.stmtQueries, req.GetStatementId())
		f.mu.Unlock()

		f.writeResponse(w, "CloseStatementResponse", message.CloseStatementResponse_builder{}.Build())
	case testWireRequestPrefix + "CloseConnectionRequest":
		f.writeResponse(w, "CloseConnectionResponse", message.CloseConnectionResponse_builder{}.Build())
	default:
		http.Error(w, "unsupported request type: "+wire.GetName(), http.StatusBadRequest)
	}
}

func (f *fakeAvaticaServer) queryForStatement(statementID uint32) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stmtQueries[statementID]
}

func (f *fakeAvaticaServer) writeResponse(w http.ResponseWriter, className string, response proto.Message) {
	wrappedResponse, err := proto.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	wire := message.WireMessage_builder{
		Name:           testWireResponsePrefix + className,
		WrappedMessage: wrappedResponse,
	}.Build()

	body, err := proto.Marshal(wire)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-google-protobuf")
	_, _ = w.Write(body)
}

func TestParameterizedQueryRowStatementLifecycle(t *testing.T) {
	resetStatementLifecycleStats()

	fakeServer := newFakeAvaticaServer(t)
	defer fakeServer.close()

	db, err := sql.Open("avatica", fakeServer.server.URL)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := context.Background()
	const iterations = 250

	for i := 0; i < iterations; i++ {
		var got int64
		err := db.QueryRowContext(ctx, "SELECT v FROM test_data WHERE id = ?", i).Scan(&got)
		if err != nil {
			t.Fatalf("QueryRowContext (value path) failed at iteration %d: %v", i, err)
		}
		if got != 1 {
			t.Fatalf("unexpected value at iteration %d: got %d, want 1", i, got)
		}
	}

	for i := 0; i < iterations; i++ {
		var got int64
		err := db.QueryRowContext(ctx, "SELECT v FROM missing_data WHERE id = ?", i).Scan(&got)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("QueryRowContext (no-rows path) failed at iteration %d: got err=%v, want %v", i, err, sql.ErrNoRows)
		}
	}

	prepareCount, closeCount := fakeServer.statementCounts()
	if prepareCount != iterations*2 {
		t.Fatalf("unexpected prepare count: got %d, want %d", prepareCount, iterations*2)
	}
	if closeCount != prepareCount {
		t.Fatalf("statement close mismatch: prepared=%d closed=%d", prepareCount, closeCount)
	}

	counters := StatementLifecycleStats()
	if counters.Opened != uint64(prepareCount) {
		t.Fatalf("unexpected statement open counter: got %d, want %d", counters.Opened, prepareCount)
	}
	if counters.CloseSucceeded != uint64(closeCount) {
		t.Fatalf("unexpected statement close success counter: got %d, want %d", counters.CloseSucceeded, closeCount)
	}
	if counters.CloseFailed != 0 {
		t.Fatalf("unexpected statement close failures: got %d, want 0", counters.CloseFailed)
	}
}

func ioReadAll(r *http.Request) ([]byte, error) {
	return io.ReadAll(r.Body)
}
