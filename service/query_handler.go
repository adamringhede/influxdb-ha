package service

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
		"net/http"
	"net/url"
		"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/influxdata/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"golang.org/x/crypto/bcrypt"
)

// Message represents a user-facing Message to be included with the Result.
type Message struct {
	Level string `json:"level"`
	Text  string `json:"text"`
}

type Result struct {
	StatementID int           `json:"statement_id"`
	Series      []*models.Row `json:"series,omitempty"`
	Messages    []*Message    `json:"messages,omitempty"`
	Partial     bool          `json:"partial,omitempty"`
	Err         string        `json:"error,omitempty"`
}

type response struct {
	Results []Result `json:"results"`
}

func parseResp(body io.Reader, chunked bool) response {
	fullResponse := response{}
	scanner := bufio.NewScanner(body)
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

func passBack(w http.ResponseWriter, res *http.Response) {
	// TODO Handle nil body when we can't get a result for influx
	defer res.Body.Close()
	for k, v := range res.Header {
		for _, h := range v {
			w.Header().Set(k, h)
		}
	}
	w.WriteHeader(res.StatusCode)
	flusher, _ := w.(http.Flusher)
	_, err := io.Copy(w, res.Body)
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

func respondWithResults(w http.ResponseWriter, results []Result) {
	w.Header().Set("Content-Type", "application/json")
	data, _ := json.Marshal(response{results})
	w.Header().Add("X-InfluxDB-Version", "relay")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(data))
}

func respondWithEmpty(w http.ResponseWriter) {
	respondWithResults(w, []Result{})
}

func handleRouteError(target string, err error) {
	if err != nil {
		log.Fatal("route "+target+": ", err)
	}
}

func handleBadRequestError(w http.ResponseWriter, err error) {
	handleErrorWithCode(w, err, http.StatusBadRequest)
}

func handleErrorWithCode(w http.ResponseWriter, err error, code int) {
	if err != nil {
		jsonError(w, code, err.Error())
		return
	}
}

func request(statement string, host string, client *http.Client, r *http.Request) ([]Result, error, *http.Response) {
	baseUrl, _ := url.Parse("http://" + host + r.URL.Path)
	queryValues := r.URL.Query()
	queryValues.Set("q", statement)
	baseUrl.RawQuery = queryValues.Encode()
	res, err := client.Post(baseUrl.String(), "", r.Body)
	results := []Result{}
	if err != nil {
		log.Println(err)
		return results, err, res
	}
	if res.StatusCode/100 != 2 {
		return results, errors.New("failed request"), res
	}
	chunked := r.URL.Query().Get("chunked") == "true"
	response := parseResp(res.Body, chunked)
	return response.Results, nil, res
}

type QueryHandler struct {
	client         *http.Client
	resolver       *cluster.Resolver
	partitioner    cluster.Partitioner
	clusterHandler *ClusterHandler
	authService    AuthService
}

func (h *QueryHandler) authenticate(r *http.Request) (*cluster.UserInfo, error) {
	if r.URL.User != nil && r.URL.User.Username() != "" {
		user := h.authService.User(r.URL.User.Username())
		if user == nil {
			return nil, meta.ErrAuthenticate
		}
		password, _ := r.URL.User.Password()
		if bcrypt.CompareHashAndPassword([]byte(user.Hash), []byte(password)) != nil {
			return nil, meta.ErrAuthenticate
		}
		return user, nil
	}
	if h.authService.HasAdmin() {
		return nil, meta.ErrAuthenticate
	}
	return nil, nil
}

func updateUser(user string, authService AuthService, updater func(info *cluster.UserInfo)) error {
	u := authService.User(user)
	if u == nil {
		return meta.ErrUserNotFound
	} else {
		updater(u)
		return authService.UpdateUser(*u)
	}
}

func HandleAdminStatement(stmt influxql.Statement, authService AuthService) (err error) {
	switch s := stmt.(type) {
	case
		*influxql.CreateUserStatement:
		err = authService.CreateUser(cluster.NewUser(s.Name, cluster.HashUserPassword(s.Password), s.Admin))
	case
		*influxql.DropUserStatement:
		err = authService.DeleteUser(s.Name)
	case
		*influxql.GrantStatement:
		err = authService.SetPrivilege(s.User, s.DefaultDatabase(), s.Privilege)
	case
		*influxql.GrantAdminStatement:
		err = updateUser(s.User, authService, func (u *cluster.UserInfo) {
			u.Admin = true
		})
	case
		*influxql.RevokeStatement:
		err = authService.RemovePrivilege(s.User, s.DefaultDatabase())
	case
		*influxql.RevokeAdminStatement:
		err = updateUser(s.User, authService, func (u *cluster.UserInfo) {
			u.Admin = false
		})
	case
		*influxql.SetPasswordUserStatement:
		err = updateUser(s.Name, authService, func (u *cluster.UserInfo) {
			u.Hash = cluster.HashUserPassword(s.Password)
		})
	}
	return
}

func (h *QueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	queryParam := r.URL.Query().Get("q")
	if queryParam != "" {
		allResults := []Result{}
		if isAdminQuery(queryParam) {
			h.clusterHandler.ServeHTTP(w, r)
			return
		}
		q, parseErr := influxql.ParseQuery(queryParam)
		db := r.URL.Query().Get("db")
		if db == "" {
			db = "default"
		}
		if parseErr != nil {
			jsonError(w, http.StatusBadRequest, "error parsing query: "+parseErr.Error())
			return
		}

		if h.authService != nil {
			user, err := h.authenticate(r)
			if err != nil {
				handleErrorWithCode(w, err, http.StatusUnauthorized)
				return
			}

			// Check privileges for all statements first.
			for _, stmt := range q.Statements {
				if !h.authService.HasAdmin() {
					if s, firstUser := stmt.(*influxql.CreateUserStatement); firstUser && s.Admin {
						// If the first statement creates the first admin user, we skip checking privileges.
						break
					}
				}
				privileges, _ := stmt.RequiredPrivileges()
				if user == nil || !isAllowed(privileges, *user, db) {
					jsonError(w, http.StatusForbidden, "forbidden statement: "+stmt.String())
					return
				}
			}

		}

		for _, stmt := range q.Statements {
			if err := HandleAdminStatement(stmt, h.authService); err != nil {
				handleBadRequestError(w, err)
				return
			}

			switch s := stmt.(type) {
			case *influxql.CreateContinuousQueryStatement,
				*influxql.CreateDatabaseStatement,
				*influxql.CreateRetentionPolicyStatement,
				*influxql.CreateSubscriptionStatement,
				*influxql.DropContinuousQueryStatement,
				*influxql.DropDatabaseStatement,
				*influxql.DropMeasurementStatement,
				*influxql.DropRetentionPolicyStatement,
				*influxql.DropSubscriptionStatement, // Need to figure out how to handle subscriptions with replication

				// Deletes could be multi-casted
				*influxql.DeleteSeriesStatement,
				*influxql.DeleteStatement,
				*influxql.DropSeriesStatement:
				// TODO Ping all replicas to make sure they are reachable before making a meta query.
				// TODO In case one request fails, store in a log in peristent storage so that the command can be replayed in order
				for _, location := range h.resolver.FindAll() {
					results, err, res := request(s.String(), location, h.client, r)
					if err != nil {
						// We may want to handle errors differently
						// Eg. with a retry,
						passBack(w, res)
						return
					}
					// Maybe only send one of the results.
					allResults = append(allResults, results...)
				}

			case *influxql.DropShardStatement,
				*influxql.KillQueryStatement,
				*influxql.ShowShardGroupsStatement,
				*influxql.ShowShardsStatement,
				*influxql.ShowStatsStatement,
				*influxql.ShowDiagnosticsStatement:

				// Not supported. Client must connect to the individual data node.
				jsonError(w, 400, "Statement is not supported on router: "+s.String())
				return

			case *influxql.ShowContinuousQueriesStatement,
				*influxql.ShowGrantsForUserStatement,
				*influxql.ShowDatabasesStatement,
				*influxql.ShowFieldKeysStatement,
				*influxql.ShowRetentionPoliciesStatement,
				*influxql.ShowSubscriptionsStatement,
				*influxql.ShowTagKeysStatement,
				*influxql.ShowUsersStatement:

				all := h.resolver.FindAll()
				for ri, location := range all {
					// Try requesting every single replica. Only if last one fails
					// return an error.
					results, err, res := request(s.String(), location, h.client, r)
					if ri == len(all)-1 && err != nil {
						passBack(w, res)
						continue
					}
					allResults = append(allResults, results...)
					break
				}

			case *influxql.ShowMeasurementsStatement,
				*influxql.ShowSeriesStatement,
				*influxql.ShowTagValuesStatement,
				*influxql.ShowQueriesStatement:

				// TODO refactor to reuse same logic as the one above
				// TODO implement merging of results

				all := h.resolver.FindAll()
				for ri, location := range all {
					// Try requesting every single replica. Only if last one fails
					// return an error.
					results, err, res := request(s.String(), location, h.client, r)
					if ri == len(all)-1 && err != nil {
						passBack(w, res)
						continue
					}
					allResults = append(allResults, results...)
					break
				}

			case *influxql.SelectStatement:

				c := &Coordinator{h.resolver, h.partitioner}
				results, err, res := c.Handle(s, r, db)
				if err != nil {
					log.Println(err)
					passBack(w, res)
					continue
				}
				allResults = append(allResults, results...)

				// TODO
				// if chunked=true, stream results to the response writer instead of
				// accumulating all of them and waiting until last is done.
				// However, the Result should only be flushed after a response
				// has been received from every shard and merged if needed.
				// http://stackoverflow.com/questions/26769626/send-a-chunked-http-response-from-a-go-server
				// Chunking with partial series works as well if grouping on time. If no grouping,
				// then partial series can either be merged into one or just flushed individually.
			}
		}

		if _, ok := h.authService.(*PersistentAuthService); ok {
			err := h.authService.(*PersistentAuthService).Save()
			handleInternalError(w, err)
			return
		}

		// TODO replace with a custom ResultFlusher that can either save all
		// results in memory and flushes in the end, or flushes every chunk received.
		respondWithResults(w, allResults)


		/*
		if len(allResults) > 0 {
			respondWithResults(w, allResults)
			return
		}
		// This is the default handler. It should never be used.
		all := h.resolver.FindAll()
		log.Printf("Resolver found the following servers: %s", strings.Join(all, ", "))
		location := all[rand.Intn(len(all))]
		log.Print("Selected host at " + location)
		baseUrl, _ := url.Parse("http://" + location + r.URL.Path)
		baseUrl.RawQuery = r.URL.Query().Encode()
		res, err := h.client.Post(baseUrl.String(), "", r.Body)
		handleRouteError("query", err)
		passBack(w, res)*/
	}

}
