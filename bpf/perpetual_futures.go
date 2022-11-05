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

package bpf

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Candle struct {
	Time      time.Time // Open time.
	Open      float64   // Open price.
	High      float64   // High price.
	Low       float64   // Low price.
	Close     float64   // Close price.
	Number    uint64    // Number of trades.
	Quantity  float64   // Total trading volume of the coin.
	Purchases float64   // Volume in coins purchased by the taker.
	Asset     float64   // Total trading volume in fiat.
	Sales     float64   // Volume in fiat sales by the taker.
}

type symbol struct {
	name  string     // The symbol name.
	table string     // The table name.
	last  *time.Time // Time of last update into database.
}

// Binance API.
var binance struct {

	// MySql data base.
	database *sql.DB

	// HTTP client for all request to Binance API.
	client *http.Client

	// Binance API key.
	key string

	// Listened symbols.
	symbols map[string]*symbol

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
func Init(key, user, password, host, database string) {

	// Binance API key.
	binance.key = key

	// HTTP client for all request to Binance API.
	binance.client = &http.Client{Timeout: time.Minute}

	// Initialize map with symbols.
	binance.symbols = make(map[string]*symbol)

	// Initialize value.
	binance.weight.last = time.Now().UTC()

	// Temporary value.
	binance.weight.maximum = 1200

	if d, e := sql.Open("mysql",
		fmt.Sprintf(
			"%s:%s@tcp(%s)/%s?allowNativePasswords=false&checkConnLiveness=false&maxAllowedPacket=0",
			user,
			password,
			host,
			database)); e == nil {

		binance.database = d

	} else {
		log.Fatalf("Error %s when open mysql database.", e)
	}

	if e := binance.database.Ping(); e != nil {
		log.Fatalf("Error %s when ping to MySql database.", e)
	}

	// Get from database the table names contain chandles.
	if r, e := binance.database.Query(fmt.Sprintf(`SHOW TABLES FROM %s LIKE 'fc_%%'`, database)); e == nil {

		var t string // Table name.

		defer r.Close()

		for r.Next() {

			if e := r.Scan(&t); e != nil {
				log.Fatalln(e)
			}

			binance.symbols[strings.ToUpper(t[3:])] = &symbol{strings.ToUpper(t[3:]), t, nil}
		}

	} else {
		log.Fatalln(e)
	}
}

// Json response from Binance perpetual future API.
func getEndpoint(endpoint string, query *map[string]string, response any) {

	// Control used weight.
	if binance.weight.used >= binance.weight.maximum {

		time.Sleep(time.Now().
			UTC().
			Truncate(time.Minute).
			Add(time.Minute).
			Sub(binance.weight.last))

	} else if binance.weight.used > 0 {

		time.Sleep(time.Microsecond * 500000)
	}

	// Create request.
	rt, er := http.NewRequest("GET", fmt.Sprintf("https://fapi.binance.com/fapi/v1/%s", endpoint), nil)
	if er != nil {
		log.Fatalf("Error %s when create new request.", er)
	}

	// Adding queries.
	if query != nil {

		qy := rt.URL.Query()

		for k, v := range *query {
			qy.Add(k, v)
		}

		rt.URL.RawQuery = qy.Encode()
	}

	// Adding headers.
	rt.Header.Add("Content-Type", "application/json")
	rt.Header.Add("X-MBX-APIKEY", binance.key)

	// Execute request.
	re, er := binance.client.Do(rt)
	if er != nil {
		log.Fatalf("Error %s when execute request.", er)
	}
	defer re.Body.Close()

	// Detect status сode.
	if re.StatusCode != 200 {
		log.Fatalf("Error status %d code.", re.StatusCode)
	} else if re.StatusCode == 429 || re.StatusCode == 418 {
		log.Fatalf("Error status code %d this request is banned.", re.StatusCode)
	}

	// Detect content type.
	if re.Header.Get("Content-Type") != "application/json" {
		log.Fatalln("Error the request returned an unexpected content type.")
	}

	// Server response time.
	if t, e := time.Parse(time.RFC1123, re.Header.Get("Date")); e == nil {
		binance.weight.last = t
	} else {
		log.Fatalf("Error %s when parse response header date.", e)
	}

	// Weight responce.
	if w, e := strconv.ParseUint(re.Header.Get("X-MBX-USED-WEIGHT-1M"), 10, 16); e == nil {
		binance.weight.used = uint16(w)
	} else {
		log.Fatalf("Error %s when getting weight.", e)
	}

	// Body from response.
	by, er := io.ReadAll(re.Body)
	if er != nil {
		log.Fatalf("Error %s when read body request.", er)
	}

	// Parse json.
	if e := json.Unmarshal(by, response); e != nil {
		log.Fatalf("Error %s when getting trades and json marshal.", e)
	}
}

// Get via Binance Futures API slice of an array candles and return this slice sorted by open time.
func getCandles(from time.Time, symbol string) []Candle {

	var jn [][]any  // Unparsed candles.
	var ce []Candle // Parsed candles.
	var oc int64    // Timestamp open candle.
	var nt uint64   // Number of trades.

	// Convert any string to float.
	af := func(v any) float64 {

		var s string

		b, e := json.Marshal(v)

		if e != nil {
			log.Fatalln(e)
		}

		if e := json.Unmarshal(b, &s); e != nil {
			log.Fatalln(e)
		}

		f, e := strconv.ParseFloat(s, 64)

		if e != nil {
			log.Fatalln(e)
		}

		return f
	}

	// Queries.
	qy := map[string]string{
		"symbol":    symbol,
		"interval":  "1m",
		"limit":     "1500",
		"startTime": strconv.FormatInt(from.UnixMilli(), 10),
	}

	// Get candles.
	getEndpoint("klines", &qy, &jn)

	// Parse candles.
	for _, r := range jn {

		if len(r) == 12 {

			// Parse open time.
			if b, e := json.Marshal(r[0]); e == nil {

				if e := json.Unmarshal(b, &oc); e != nil {
					log.Fatalln(e)
				}
			} else {
				log.Fatal(e)
			}

			// Parse number of trades.
			if b, e := json.Marshal(r[8]); e == nil {

				if e := json.Unmarshal(b, &nt); e != nil {
					log.Fatalln(e)
				}
			} else {
				log.Fatal(e)
			}

			ce = append(ce, Candle{
				Time:      time.UnixMilli(oc).UTC(), // Open time.
				Open:      af(r[1]),                 // Open price.
				High:      af(r[2]),                 // High price.
				Low:       af(r[3]),                 // Low price.
				Close:     af(r[4]),                 // Close price.
				Number:    nt,                       // Number of trades.
				Quantity:  af(r[5]),                 // Volume.
				Purchases: af(r[9]),                 // Taker buy base asset volume.
				Asset:     af(r[7]),                 // Quote asset volume.
				Sales:     af(r[10]),                // Taker buy quote asset volume.
			})

		} else {
			log.Fatalln("Unreadable kline data.")
		}
	}

	// Sort array by open time
	sort.Slice(ce, func(i, j int) bool {
		return ce[i].Time.Before(ce[j].Time)
	})

	return ce
}

// Receives via function "getCandles" candles and in new loop shift time to the end of the slice from previous loop.
func receiveCandles(from time.Time, symbol string, loop func(candles []Candle)) {

	// In every new loop shift time to the end of the slice from previous loop.
	for cs := getCandles(from, symbol); len(cs) > 0; cs = getCandles(cs[len(cs)-1].Time.Truncate(time.Minute).Add(time.Minute), symbol) {

		cl := len(cs)

		// Check that the last minute is closed.
		if cs[cl-1].Time.Truncate(time.Minute).Before(binance.weight.last.Truncate(time.Minute)) {
			loop(cs)
		} else if cl-1 > 0 {
			loop(cs[:cl-1])
		}
	}
}

// Receives candles via function "receiveCandles" and save to database.
func saveCandles(from time.Time, symbol string) {

	// Receive candles.
	receiveCandles(from, symbol, func(ce []Candle) {

		// Prepared data to be inserted into the database.
		da := make([]any, 0)

		// MySQL query.
		qy := fmt.Sprintf(`INSERT INTO fc_%s (time,
			open,
			high,
			low,
			close,
			number,
			quantity,
			purchases,
			asset,
			sales) VALUES`, strings.ToLower(symbol))

		// Preparation data before inserting into the database.
		for i := 0; i < len(ce); i++ {

			da = append(da, ce[i].Time.Format("2006-01-02 15:04:05"))
			da = append(da, ce[i].Open)
			da = append(da, ce[i].High)
			da = append(da, ce[i].Low)
			da = append(da, ce[i].Close)
			da = append(da, ce[i].Number)
			da = append(da, ce[i].Quantity)
			da = append(da, ce[i].Purchases)
			da = append(da, ce[i].Asset)
			da = append(da, ce[i].Sales)

			if i == len(ce)-1 {
				qy += "(?,?,?,?,?,?,?,?,?,?)"
			} else {
				qy += "(?,?,?,?,?,?,?,?,?,?),"
			}
		}

		// Inserting candles to database.
		if r, e := binance.database.Exec(qy, da...); e != nil {
			log.Fatal(e, r)
		}
	})
}

// Check the existence of candles in the database, if there
// candles is exist get the last time of the saved candle and returns
// time: last saved candle, bool: existence of candles.
func candleTimeOfLastSaved(symbol string) (time.Time, bool) {

	var nc uint64    // Number candles in database.
	var st string    // String time from database.
	var te time.Time // Parsed time.

	// Symbol name to lower case.
	symbol = strings.ToLower(symbol)

	// Check the existence of candles in the database.
	if e := binance.database.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM fc_%s", symbol)).Scan(&nc); e != nil {
		log.Fatalln(e)
	} else if nc == 0 {
		return te, false
	}

	// Select from database last saved candle.
	if e := binance.database.QueryRow(fmt.Sprintf("SELECT time FROM fc_%s ORDER BY time DESC LIMIT 1", symbol)).Scan(&st); e != nil {
		log.Fatalln(e)
	}

	// Parse string to time.
	if t, e := time.Parse("2006-01-02 15:04:05", st); e == nil {
		te = t
	} else {
		log.Fatalln(e)
	}

	return te, true
}

// Actualize candles in database via function: candlesSaveIntoDatabase.
func updateCandles(from time.Time, symbol string) {

	if t, b := candleTimeOfLastSaved(symbol); b {
		from = t.Add(time.Minute)
	}

	saveCandles(from, symbol)
}

// Selected candles from database and returns their in array.
func selectCandles(from time.Time, symbol string) []Candle {

	var pc []Candle // Prepared candles for returns.
	var ce Candle   // Candle for scan the database.
	var st string   // Time in string for scan the database.

	qy := fmt.Sprintf(`SELECT time,
		open,
		high,
		low,
		close,
		number,
		quantity,
		purchases,
		asset,
		sales FROM fc_%s WHERE time >= ? ORDER BY time ASC`, strings.ToLower(symbol))

	rw, er := binance.database.Query(qy, from.Format("2006-01-02 15:04"))
	if er != nil {
		log.Fatalln(er)
	}
	defer rw.Close()

	for rw.Next() {

		if e := rw.Scan(&st, &ce.Open, &ce.High, &ce.Low, &ce.Close, &ce.Number, &ce.Quantity, &ce.Purchases, &ce.Asset, &ce.Sales); e != nil {
			log.Fatalln(e)
		}

		if t, e := time.Parse("2006-01-02 15:04:05", st); e == nil {
			ce.Time = t
		} else {
			log.Fatalln(e)
		}

		pc = append(pc, ce)
	}

	return pc
}
