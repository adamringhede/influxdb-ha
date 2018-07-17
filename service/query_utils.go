package service

import (
	"github.com/influxdata/influxdb/models"
	"bufio"
	"net/http"
	"io"
	"fmt"
	"net/url"
	"log"
	"encoding/json"
	"errors"
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