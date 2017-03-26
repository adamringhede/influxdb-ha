package router

import (
	"net/http"
	"log"
	"io"
	"net/url"
	"math/rand"
	"time"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb-relay/relay"
	"fmt"
	"github.com/influxdata/influxdb/models"
	"encoding/json"
	"errors"
	/*"io/ioutil"
	"strings"
	"bufio"
	"os"
	"github.com/derekparker/delve/dwarf/reader"*/
	"bufio"
)

// Message represents a user-facing message to be included with the result.
type message struct {
	Level string `json:"level"`
	Text  string `json:"text"`
}

type result struct {
	StatementID int           `json:"statement_id"`
	Series      []*models.Row `json:"series,omitempty"`
	Messages    []*message    `json:"messages,omitempty"`
	Partial     bool          `json:"partial,omitempty"`
	Err         string        `json:"error,omitempty"`
}

type response struct {
	Results	[]result	`json:"results"`
}

func parseResp(resp *http.Response, chunked bool) response {
	fullResponse := response{}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		var r response
		err := json.Unmarshal([]byte(scanner.Text()), &r)
		if err != nil {
			log.Panic(err)
		}
		fullResponse.Results = append(fullResponse.Results, r.Results...)
	}
	return fullResponse
}

func passBack(w *http.ResponseWriter, res *http.Response) {
	defer res.Body.Close()
	for k, v := range res.Header {
		for _, h := range v {
			(*w).Header().Set(k, h)
		}
	}
	(*w).WriteHeader(res.StatusCode)
	flusher, _ := (*w).(http.Flusher)
	_, err := io.Copy(*w, res.Body)
	flusher.Flush()
	if err != nil {
		log.Fatal(err)
	}
}

func jsonError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	data := fmt.Sprintf("{\"error\":%q}\n", message)
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	w.WriteHeader(code)
	w.Write([]byte(data))
}

func respondWithResults(w *http.ResponseWriter, results []result) {
	(*w).Header().Set("Content-Type", "application/json")
	data, _ := json.Marshal(response{results})
	(*w).Header().Add("X-InfluxDB-Version", "relay")
	(*w).WriteHeader(http.StatusOK)
	(*w).Write([]byte(data))
}

func handleRouteError(target string, err error) {
	if err != nil {
		log.Fatal("route " + target + ": ", err)
	}
}

func request(statement influxql.Statement, host string, client *http.Client, r *http.Request) ([]result, error, *http.Response) {
	baseUrl, _ := url.Parse("http://" + host + r.URL.Path)
	queryValues := r.URL.Query()
	queryValues.Set("q", statement.String())
	baseUrl.RawQuery = r.URL.Query().Encode()
	res, err := client.Post(baseUrl.String(), "", r.Body)
	results := []result{}
	if err != nil {
		return results, err, res
	}
	if res.StatusCode/100 != 2 {
		return results, errors.New("Failed request"), res
	}
	chunked := r.URL.Query().Get("chunked") == "true"
	response := parseResp(res, chunked)
	return response.Results, nil, res
}

type Resolver interface {
	GetDataNodes()
}
type HTTPHandler struct {
	client 		*http.Client
	config 		*RouterConfig
	replicaSets 	[]ReplicaSet
}
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	/*buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal("request",err)
	}*/
	log.Printf("Received request %s?%s\n", r.URL.Path, r.URL.RawQuery)
	if r.URL.Path == "/write" {

		// TODO Need to refactor the creation of relays
		// TODO Find shard key for the specified database and measurement
		// TODO Hash tags as necessary and add as additional tags. If a tag is not included which is needed for the shard key return an error
		// TODO Select the correct replicaset based on sharding or use the default one (first)

		rs := h.replicaSets[0]
		outputs := []relay.HTTPOutputConfig{}
		for _, replica := range rs.Replicas {
			output := relay.HTTPOutputConfig{}
			output.Name = replica.Name
			output.Location = "http://" + replica.Location + "/write"
			outputs = append(outputs, output)
		}

		relayHttpConfig := relay.HTTPConfig{}
		relayHttpConfig.Name = rs.Name
		relayHttpConfig.Outputs = outputs
		relayHttp, relayErr := relay.NewHTTP(relayHttpConfig)
		if relayErr != nil {
			log.Panic(relayErr)
		}
		relayHttp.(*relay.HTTP).ServeHTTP(w, r)

	} else {
		// send to any replica
		hosts := h.replicaSets[0].Replicas

		// when the selected measurement is sharded, the correct replicaset needs to be found
		// (non sharded measurements are only on one replicaset and need to lookup in a config server what shard they are on)
		// a set of hosts need to be resolved for the shard's replicate
		// the shared is selected based on the given shard key.
		// if the query does not match the any shard key,

		// TODO Find the chunk based on a shard key matching the query Option.
		// TODO Get the replicaset hosts of the shard holding that shard
		// TODO Select one of those hosts and pass on the query.



		queryParam := r.URL.Query().Get("q")
		if queryParam != "" {
			allResults := []result{}
			q, parseErr := influxql.ParseQuery(r.URL.Query()["q"][0])
			if parseErr != nil {
				jsonError(w, http.StatusBadRequest, "error parsing query: " + parseErr.Error())
				return
			}
			log.Printf("There are %d statements in the query", len(q.Statements))
			//otherStatements := influxql.Statements{}
			for _, stmt := range q.Statements {

				// TODO append all statements on a slice with the corresponding handler
				// so that results can be returned in the correct order.
				// The handler should return a slice of Result.
				switch s := stmt.(type) {
				case 	*influxql.CreateContinuousQueryStatement,
					*influxql.CreateDatabaseStatement,
					*influxql.CreateRetentionPolicyStatement,
					*influxql.CreateSubscriptionStatement,
					*influxql.CreateUserStatement,
					*influxql.DropContinuousQueryStatement,
					*influxql.DropDatabaseStatement,
					*influxql.DropMeasurementStatement,
					*influxql.DropRetentionPolicyStatement,
					*influxql.DropSubscriptionStatement,
					*influxql.DropUserStatement,
					*influxql.GrantStatement,
					*influxql.GrantAdminStatement,
					*influxql.RevokeStatement,
					*influxql.RevokeAdminStatement,
					*influxql.SetPasswordUserStatement,
					// Deletes could be multi-casted
					*influxql.DeleteSeriesStatement,
					*influxql.DeleteStatement,
					*influxql.DropSeriesStatement:
					// Send the statement to every replica in every replicaset.
					// Keep track of failed queries.
					log.Print("Broadcast: " + s.String())

					// TODO change config file so that this can be changed to send to all RSs
					for _, replica := range hosts {
						results, err, res := request(s, replica.Location, h.client, r)
						if err != nil {
							// We may want to handle errors differently
							// Eg. with a retry,
							passBack(&w, res)
							return
						}
						// Maybe only send one of the results.
						allResults = append(allResults, results...)
					}


				case 	*influxql.DropShardStatement,
					*influxql.KillQueryStatement,
					*influxql.ShowShardGroupsStatement,
					*influxql.ShowShardsStatement,
					*influxql.ShowStatsStatement,
					*influxql.ShowDiagnosticsStatement:

					// Not supported. Return an error. Client must connect to the
					// individual data node.
					jsonError(w, 400, "Statement is not supported on router: " + s.String())
					return

				case	*influxql.ShowContinuousQueriesStatement,
					*influxql.ShowGrantsForUserStatement,
					*influxql.ShowDatabasesStatement,
					*influxql.ShowFieldKeysStatement,
					*influxql.ShowRetentionPoliciesStatement,
					*influxql.ShowSubscriptionsStatement,
					*influxql.ShowTagKeysStatement,
					*influxql.ShowUsersStatement:

					// Send the query to primary replicaset, any replica

					for ri, replica := range h.replicaSets[0].Replicas {
						// Try requesting every single replica. Only if last one fails
						// return an error.
						results, err, res := request(s, replica.Location, h.client, r)
						if ri == len(h.replicaSets[0].Replicas)-1 && err != nil {
							passBack(&w, res)
							continue
						}
						allResults = append(allResults, results...)
						break
					}

				case	*influxql.ShowMeasurementsStatement,
					*influxql.ShowSeriesStatement,
					*influxql.ShowTagValuesStatement,
					*influxql.ShowQueriesStatement:

					// Send to all replicasets, any replica, merge returned values

					// TODO change to use all replicasets
					// TODO refactor to reuse same logic as the one above
					// TODO implement merging of results
					for ri, replica := range h.replicaSets[0].Replicas {
						// Try requesting every single replica. Only if last one fails
						// return an error.
						results, err, res := request(s, replica.Location, h.client, r)
						if ri == len(h.replicaSets[0].Replicas)-1 && err != nil {
							passBack(&w, res)
							continue
						}
						allResults = append(allResults, results...)
						break
					}

				case *influxql.SelectStatement:
					log.Print("Sharding query: " + s.String())
					selectedHost := hosts[rand.Intn(len(hosts))]
					results, err, res := request(s, selectedHost.Location, h.client, r)
					if err != nil {
						passBack(&w, res)
						continue
					}
					allResults = append(allResults, results...)

					// TODO
					// if chunked=true, stream results to the response writer instead of
					// accumulating all of them and waiting until last is done.
					// However, the result should only be flushed after a response
					// has been received from every shard and merged if needed.
					// http://stackoverflow.com/questions/26769626/send-a-chunked-http-response-from-a-go-server
				}
			}
			if len(allResults) > 0 {
				respondWithResults(&w, allResults)
				return
			}
		}

		// TODO if creating a database, use a post request instead.
		// Create different types of handlers for different statements
		//

		/*
		func (*AlterRetentionPolicyStatement) node()  {}


		 */

		/*
		if the query includes a statement for a sharded measurement,
		then that query needs to be processed separately and appended to the result.
		 */


		/*
		this need to be fixed. need to decide what statements should be
		sent to all hosts. read statements should only be sent to the first
		host. things that are specific to each host should not be allowed
		to access through the router, such as dropping a shard. or we implement
		support for those statements as well. some like SHOW SERIES should
		be sent to all shards and be concatenated, but only to one host in each
		replicaset.
		 */

		/*if len(otherStatements) > 0 {
			for i, host := range hosts {
				baseUrl, _ := url.Parse("http://" + host + r.URL.Path)
				queryValues := r.URL.Query()
				queryValues.Set("q", otherStatements.String())
				baseUrl.RawQuery = queryValues.Encode()
				res, err := client.Get(baseUrl.String())
				handleRouteError("query", err)
				if i == len(hosts) - 1 {
					passBack(&w, res)
				}
			}
		}*/

		// This is the default handler.
		selectedHost := hosts[rand.Intn(len(hosts))]
		log.Print("Selected host " + selectedHost.Name + " at " + selectedHost.Location)
		baseUrl, _ := url.Parse("http://" + selectedHost.Location + r.URL.Path)
		//queryValues := r.URL.Query()
		//queryValues.Set("q", selectStatement)
		baseUrl.RawQuery = r.URL.Query().Encode()
		res, err := h.client.Post(baseUrl.String(), "", r.Body)
		handleRouteError("query", err)
		passBack(&w, res)
	}
}


func Start(config *RouterConfig, replicaSets []ReplicaSet) {
	addr := ":5096"
	if config.BindAddr != "" {
		addr = config.BindAddr
	}
	client := &http.Client{Timeout: 10 * time.Second}
	http.Handle("/", &HTTPHandler{client, config, replicaSets})

	log.Println("Listening on " + addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
