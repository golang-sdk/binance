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
	"sort"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Binance API.
var api struct {

	// MySql data base.
	database *sql.DB

	// HTTP client for all request to Binance API.
	client *http.Client

	// Binance API key.
	key string

	// Used weight.
	weight struct {

		// Time last response from the API server.
		last time.Time

		// Maximum allowable weight.
		maximum uint16

		// Count used weight.
		used uint16
	}
}

// Initialize default values for connect API, prepare and ping connect to database.
func Init(key, user, password, address, database string) {

	api.key = key
	api.client = &http.Client{Timeout: time.Minute}

	api.weight.last = time.Now().UTC()
	api.weight.maximum = 1200

	if d, e := sql.Open("mysql",
		fmt.Sprintf(
			"%s:%s@tcp(%s)/%s?allowNativePasswords=false&checkConnLiveness=false&maxAllowedPacket=0",
			user,
			password,
			address,
			database)); e == nil {

		api.database = d

	} else {
		log.Fatalf("Error %s when open mysql database.", e)
	}

	if e := api.database.Ping(); e != nil {
		log.Fatalf("Error %s when ping to MySql database.", e)
	}
}

// Json response from Binance perpetual future API.
func requestEndpoint(endpoint string, queries *map[string]string, response any) {

	// Control used weight.
	if api.weight.used >= api.weight.maximum {

		time.Sleep(time.Now().
			UTC().
			Truncate(time.Minute).
			Add(time.Minute).
			Sub(api.weight.last))

	} else if api.weight.used > 0 {

		time.Sleep(time.Second)
	}

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
	if t, e := time.Parse(time.RFC1123, res.Header.Get("Date")); e == nil {
		api.weight.last = t
	} else {
		log.Fatalf("Error %s when parse response header date.", e)
	}

	// Weight responce.
	if w, e := strconv.ParseUint(res.Header.Get("X-MBX-USED-WEIGHT-1M"), 10, 16); e == nil {
		api.weight.used = uint16(w)
	} else {
		log.Fatalf("Error %s when getting weight.", e)
	}

	// Body from response.
	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("Error %s when read body request.", err)
	}

	// Parse json.
	if e := json.Unmarshal(body, response); e != nil {
		log.Fatalf("Error %s when getting trades and json marshal.", e)
	}
}

// Getted and return slice trades.
func getSliceHistoricalTrades(symbol string, fromId uint64) []Trade {

	// Transactions from API responce.
	var jtr []struct {
		ID           uint64 `json:"id"`
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
		"limit":  "100",
		"fromId": strconv.FormatUint(fromId, 10),
	}

	// Request historical trades.
	requestEndpoint("historicalTrades", &que, &jtr)

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

	return tra
}

// Getting all historical trades splitted by slices, in one slice conines thousand transactions.
func getAllHistoricalTrades(symbol string, fromId uint64, slice func(trades []Trade)) {

	for {

		t := getSliceHistoricalTrades(symbol, fromId)

		log.Println("len", len(t))

		// Sort array by trade id that get last trade id.
		sort.Slice(t, func(i, j int) bool {
			return t[i].TradeId < t[j].TradeId
		})

		if len(t) > 0 {

			// Call hook.
			slice(t)

			// Offset from id by the limit.
			fromId = t[len(t)-1].TradeId + 1
		}
	}
}

// Getting and save historical trades.
func saveHistoricalTrades(symbol string, fromId uint64) {

	// Last trade ID of trades.
	var lastID uint64

	// Last time of trades.
	var lastTime time.Time

	// Getting historical trades.
	getAllHistoricalTrades(symbol, fromId, func(trades []Trade) {

		q := "INSERT INTO %s (id, time, price, qty, quote_qty, is_buyer_maker) VALUES"

		p := make([]any, 0)

		for i := 0; i < len(trades); i++ {

			p = append(p, trades[i].TradeId)
			p = append(p, trades[i].Time.Format("2006-01-02 15:04:05.000"))
			p = append(p, trades[i].Price)
			p = append(p, trades[i].Qty)
			p = append(p, trades[i].QuoteQty)
			p = append(p, trades[i].IsBuyerMaker)

			if i == len(trades)-1 {
				q += "(?,?,?,?,?,?)"
			} else {
				q += "(?,?,?,?,?,?),"
			}

			lastID = trades[i].TradeId
			lastTime = trades[i].Time
		}

		// Save trades to database.
		if r, e := api.database.Exec(fmt.Sprintf(q, "ft_btcusdt"), p...); e != nil {
			log.Fatal(e, r)
		}

		// Save information of last saved trade.
		if r, e := api.database.Exec("UPDATE ft_last_saved SET tid = ?, time = ? WHERE symbol = ?",
			lastID,
			lastTime.Format("2006-01-02 15:04:05.000"),
			symbol); e != nil {
			log.Fatal(e, r)
		}
	})
}
