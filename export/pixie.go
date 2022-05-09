package export

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"moul.io/http2curl"
	"px.dev/pxapi"
	"px.dev/pxapi/errdefs"
	"px.dev/pxapi/types"
)

type PixieToCurl struct {
	apiToken   string   // get this from the pixie UI
	clusterID  string   // get this from the pixie UI
	destFilter string   // substring match filter of destinations to accept
	maxRows    int      // limt to the # of request we can receive from Pixie
	baseURL    *url.URL // base URL to prepend to the path (usually http://some_service)
	startTime  string
}

func NewPixieToCurl(apiToken, clusterID, destFilter, baseURL string) PixieToCurl {
	u, err := url.Parse(baseURL)
	mustNotError(err, "invalid base url format")

	return PixieToCurl{
		apiToken:   apiToken,
		clusterID:  clusterID,
		destFilter: destFilter,
		maxRows:    2000,
		startTime:  "-5m",
		baseURL:    u,
	}
}

// Run connects to the Pixie API and begins export.
//
// Follows examples from here: https://docs.pixielabs.ai/using-pixie/api-quick-start/
// You can also extract this data from the command line using commands like:
//
// px run px/http_data -o json
func (r PixieToCurl) Run() error {
	// Create a Pixie client.
	ctx := context.Background()
	client, err := pxapi.NewClient(ctx, pxapi.WithAPIKey(r.apiToken))
	mustNotError(err, "failed to create new Pixie Client")

	// Create a connection to the cluster.
	vz, err := client.NewVizierClient(ctx, r.clusterID)
	mustNotError(err, "failed to create Vizier Client")

	// Create TableMuxer to accept results table.
	tm := &tableMux{cfg: r}
	// Execute the PxL script.
	resultSet, err := vz.ExecuteScript(ctx, r.script(), tm)
	if err != nil && err != io.EOF {
		panic(err)
	}

	// Receive the PxL script results and convert to curl output
	defer resultSet.Close()
	if err := resultSet.Stream(); err != nil {
		if errdefs.IsCompilationError(err) {
			fmt.Printf("Got compiler error: \n %s\n", err.Error())
		} else {
			fmt.Printf("Got error : %+v, while streaming\n", err)
		}
	}

	return nil
}

func (r PixieToCurl) script() string {
	// this test harness can be used for a variety of things so let's make the column list variable
	columns := strings.Join(queryColumns(), "', '")
	return `
import px

def add_source_dest_columns(df):
    ''' Add source and destination columns for the HTTP request.

    HTTP requests are traced server-side (trace_role==2), unless the server is
    outside of the cluster in which case the request is traced client-side (trace_role==1).

    When trace_role==2, the HTTP request source is the remote_addr column
    and destination is the pod column. When trace_role==1, the HTTP request
    source is the pod column and the destination is the remote_addr column.

    Input DataFrame must contain trace_role, upid, remote_addr columns.
    '''
    df.pod = df.ctx['pod']
    df.namespace = df.ctx['namespace']

    # If remote_addr is a pod, get its name. If not, use IP address.
    df.ra_pod = px.pod_id_to_pod_name(px.ip_to_pod_id(df.remote_addr))
    df.is_ra_pod = df.ra_pod != ''
    df.ra_name = px.select(df.is_ra_pod, df.ra_pod, df.remote_addr)

    df.is_server_tracing = df.trace_role == 2
    df.is_source_pod_type = px.select(df.is_server_tracing, df.is_ra_pod, True)
    df.is_dest_pod_type = px.select(df.is_server_tracing, True, df.is_ra_pod)

    # Set source and destination based on trace_role.
    df.source = px.select(df.is_server_tracing, df.ra_name, df.pod)
    df.destination = px.select(df.is_server_tracing, df.pod, df.ra_name)

    # Filter out messages with empty source / destination.
    df = df[df.source != '']
    df = df[df.destination != '']

    df = df.drop(['ra_pod', 'is_ra_pod', 'ra_name', 'is_server_tracing'])

    return df

df = px.DataFrame('http_events', start_time='` + r.startTime + `')
df = add_source_dest_columns(df)
df = df[px.contains(df.destination, '` + r.destFilter + `')]
df = df[['` + columns + `']]
df = df.head(` + fmt.Sprintf("%v", r.maxRows) + `)

px.display(df, 'http')
`
}

func queryColumns() []string {
	return []string{
		"req_path",
		"remote_addr",
		"req_method",
		"req_headers",
		"req_body",
		"resp_status",
		"major_version",
		"destination",
	}
}

// Satisfies the TableRecordHandler interface.
type tablePrinter struct {
	cfg PixieToCurl
}

func (t *tablePrinter) HandleInit(ctx context.Context, metadata types.TableMetadata) error {
	return nil
}
func (t *tablePrinter) HandleRecord(ctx context.Context, r *types.Record) error {
	// convert the pixie return into an http.Request so we can use an off the shelf lib to convert to curl
	// pixie API does not include metadata in response but it does return columns in the correct order
	expectedCols := len(queryColumns())
	if len(r.Data) < expectedCols {
		fmt.Printf("not enough columns were returned by pixie API received: %d expected: %d\n", len(r.Data), expectedCols)
		os.Exit(1)
	}

	// bodies are only returned as strings so we need to reconvert them
	// keep in mind that Pixie has a default 512 byte limit for speed - you can override by setting
	// a larger value using PL_DATASTREAM_BUFFER_SIZE
	// https://github.com/pixie-io/pixie/commit/082fd123281f1ec8bfa7fff5d4b631672760c27f
	var body io.Reader
	bodyStr := r.Data[4].String()
	if len(bodyStr) == 0 {
		// set it to nil so we don't get empty -d `` in our curl statement
		body = nil
	} else {
		body = bytes.NewReader([]byte(bodyStr))
	}
	url, _ := url.Parse(t.cfg.baseURL.String())
	url.Path = path.Join(t.cfg.baseURL.Path, r.Data[0].String())
	req, err := http.NewRequest(r.Data[2].String(), url.String(), body)
	mustNotError(err, "failed to create http.Request")

	// headers come in as a JSON map so we'll need to do some conversion
	headers := make(map[string]string)
	err = json.Unmarshal([]byte(r.Data[3].String()), &headers)
	mustNotError(err, "failed to unmarshal headers JSON")
	for key, val := range headers {
		req.Header.Add(key, val)
	}

	// dump the curl statement to stdout
	cmd, err := http2curl.GetCurlCommand(req)
	mustNotError(err, "failed to convert http.Request to curl")

	output := cmd.String()
	// if we are expecting a status code 200 let's add extra checking to curl
	if r.Data[5].String() == "200" {
		output += " --fail"
	}
	fmt.Println(output)

	return nil
}
func (t *tablePrinter) HandleDone(ctx context.Context) error {
	return nil
}

// Satisfies the TableMuxer interface.
type tableMux struct {
	cfg PixieToCurl
}

func (s *tableMux) AcceptTable(ctx context.Context, metadata types.TableMetadata) (pxapi.TableRecordHandler, error) {
	return &tablePrinter{cfg: s.cfg}, nil
}

func mustNotError(err error, txt string) {
	if err != nil {
		fmt.Printf("%v: %v\n", txt, err.Error())
		os.Exit(1)
	}
}
