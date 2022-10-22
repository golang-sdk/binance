// Copyright 2022 Vasiliy Vdovin

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fapi

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Binance API.
var api struct {

	// MySql data base.
	db *sql.DB

	// HTTP client for all request to Binance API.
	client *http.Client

	// Binance API key.
	key string
}

// Initialize default values for connect API, prepare and ping connect to database.
func Init(key, user, password, address, database string) {

	api.key = key
	api.client = &http.Client{Timeout: time.Minute}

	if db, err := sql.Open("mysql",
		fmt.Sprintf(
			"%s:%s@tcp(%s)/%s?allowNativePasswords=false&checkConnLiveness=false&maxAllowedPacket=0",
			user,
			password,
			address,
			database)); err == nil {

		api.db = db

	} else {
		log.Fatalf("Error %s when open mysql database.", err)
	}

	if e := api.db.Ping(); e != nil {
		log.Fatalf("Error %s when ping to MySql database.", e)
	}
}

// Universal getting json from Binance perpetual future API.
func requestEndpoint(endpoint string, queries *map[string]string, response any) (time.Time, int64) {

	// Create request.
	req, err := http.NewRequest("GET", fmt.Sprintf("https://fapi.binance.com/fapi/v1/%s", endpoint), nil)
	if err != nil {
		log.Fatalf("Error %s when create new request.", err)
	}

	// Adding queries.
	if is := queries != nil; is && len(*queries) > 0 {

		que := req.URL.Query()

		for k, v := range *queries {
			que.Add(k, v)
		}

		req.URL.RawQuery = que.Encode()
	}

	// Adding headers.
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-MBX-APIKEY", api.key)

	// Execute request.
	res, err := api.client.Do(req)
	if err != nil {
		log.Fatalf("Error %s when execute request.", err)
	}
	defer res.Body.Close()

	// Detect status сode.
	if res.StatusCode != 200 {
		log.Fatalf("Error status %d code.", res.StatusCode)
	} else if res.StatusCode == 429 || res.StatusCode == 418 {
		log.Fatalf("Error status code %d this request is banned.", res.StatusCode)
	}

	// Detect content type.
	if res.Header.Get("Content-Type") != "application/json" {
		log.Fatalln("Error the request returned an unexpected content type.")
	}

	// Server response time.
	dat, err := time.Parse(time.RFC1123, res.Header.Get("Date"))
	if err != nil {
		log.Fatalf("Error %s when parse response header date.", err)
	}

	// Weight responce.
	weg, err := strconv.ParseInt(res.Header.Get("X-MBX-USED-WEIGHT-1M"), 10, 64)
	if err != nil {
		log.Fatalf("Error %s when getting weight.", err)
	}

	// Body from response.
	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("Error %s when read body request.", err)
	}

	// Parse json.
	if err := json.Unmarshal(body, response); err != nil {
		log.Fatalf("Error %s when getting trades and json marshal.", err)
	}

	return dat, weg
}

// Returns trades, server response time and weight 1m
func getHistoricalTrades(symbol string, fromId int64) ([]Trade, time.Time, int64) {

	// Transactions from API responce.
	var jtr []struct {
		ID           int64  `json:"id"`
		Price        string `json:"price"`
		Qty          string `json:"qty"`
		QuoteQty     string `json:"quoteQty"`
		Time         int64  `json:"time"`
		IsBuyerMaker bool   `json:"isBuyerMaker"`
	}

	// Parsed transactions.
	tra := make([]Trade, 0)

	// Queries.
	que := map[string]string{
		"symbol": symbol,
		"limit":  "1000",
		"fromId": strconv.FormatInt(fromId, 10),
	}

	// Request historical trades.
	dat, weg := requestEndpoint("historicalTrades", &que, &jtr)

	// Fields type conversion.
	for _, t := range jtr {

		trade := Trade{
			TradeId:      t.ID,
			Time:         time.UnixMilli(t.Time).UTC(),
			IsBuyerMaker: t.IsBuyerMaker,
		}

		// Price convert string to float.
		if f, e := strconv.ParseFloat(t.Price, 64); e == nil {
			trade.Price = f
		} else {
			log.Fatalf("Error %s when convert type price.", e)
		}

		// Qty convert string to float.
		if f, e := strconv.ParseFloat(t.Qty, 64); e == nil {
			trade.Qty = f
		} else {
			log.Fatalf("Error %s when convert type qty.", e)
		}

		// Quote qty convert string to float.
		if f, e := strconv.ParseFloat(t.QuoteQty, 64); e == nil {
			trade.QuoteQty = f
		} else {
			log.Fatalf("Error %s when convert type quote qty.", e)
		}

		tra = append(tra, trade)
	}

	return tra, dat, weg
}
