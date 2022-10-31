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
	"strings"
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
func Init(key, user, password, host, database string) {

	api.key = key
	api.client = &http.Client{Timeout: time.Minute}

	api.weight.last = time.Now().UTC()
	api.weight.maximum = 1200

	if d, e := sql.Open("mysql",
		fmt.Sprintf(
			"%s:%s@tcp(%s)/%s?allowNativePasswords=false&checkConnLiveness=false&maxAllowedPacket=0",
			user,
			password,
			host,
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
func requestEndpoint(endpoint string, query *map[string]string, response any) {

	// Control used weight.
	if api.weight.used >= api.weight.maximum {

		time.Sleep(time.Now().
			UTC().
			Truncate(time.Minute).
			Add(time.Minute).
			Sub(api.weight.last))

	} else if api.weight.used > 0 {

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
	rt.Header.Add("X-MBX-APIKEY", api.key)

	// Execute request.
	re, er := api.client.Do(rt)
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
		api.weight.last = t
	} else {
		log.Fatalf("Error %s when parse response header date.", e)
	}

	// Weight responce.
	if w, e := strconv.ParseUint(re.Header.Get("X-MBX-USED-WEIGHT-1M"), 10, 16); e == nil {
		api.weight.used = uint16(w)
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

// Receives via Binance Futures API slice of an array candles and return this slice sorted by open time.
func candlesSlice(from time.Time, symbol string) []Candle {

	// Unparsed candles.
	var jn [][]any

	// Parsed candles.
	var ce []Candle

	// Queries.
	qy := map[string]string{
		"symbol":    symbol,
		"interval":  "1m",
		"limit":     "1500",
		"startTime": strconv.FormatInt(from.UnixMilli(), 10),
	}

	// Request and getting klines data.
	requestEndpoint("klines", &qy, &jn)

	// Parse candles.
	for _, r := range jn {

		if len(r) == 12 {

			ce = append(ce, Candle{
				Time:      time.UnixMilli(convertAnyToInt(r[0])).UTC(),     // Open time.
				Open:      convertStringToFloat(convertAnyToString(r[1])),  // Open price.
				High:      convertStringToFloat(convertAnyToString(r[2])),  // High price.
				Low:       convertStringToFloat(convertAnyToString(r[3])),  // Low price.
				Close:     convertStringToFloat(convertAnyToString(r[4])),  // Close price.
				Number:    convertAnyToUint(r[8]),                          // Number of trades.
				Quantity:  convertStringToFloat(convertAnyToString(r[5])),  // Volume.
				Purchases: convertStringToFloat(convertAnyToString(r[9])),  // Taker buy base asset volume.
				Asset:     convertStringToFloat(convertAnyToString(r[7])),  // Quote asset volume.
				Sales:     convertStringToFloat(convertAnyToString(r[10])), // Taker buy quote asset volume.
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

// Receives via function "candlesSlice" candles and in new loop shift time to the end of the slice from previous loop.
func candlesLoops(from time.Time, symbol string, loop func(candles []Candle)) {

	// In every new loop shift time to the end of the slice from previous loop.
	for cs := candlesSlice(from, symbol); len(cs) > 0; cs = candlesSlice(cs[len(cs)-1].Time.Truncate(time.Minute).Add(time.Minute), symbol) {

		// Check that the last minute is closed.
		if cs[len(cs)-1].Time.Truncate(time.Minute).Before(api.weight.last.Truncate(time.Minute)) {
			loop(cs)
		} else {
			loop(cs[:len(cs)-1])
		}
	}
}

// Receives candles via function "candlesLoops" and save to database.
func candlesSaveIntoDatabase(from time.Time, symbol string) {

	// Receive candles.
	candlesLoops(from, symbol, func(ce []Candle) {

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

		// Inserting klines to database.
		if r, e := api.database.Exec(qy, da...); e != nil {
			log.Fatal(e, r)
		}
	})
}

// Check the existence of candles in the database, if there
// candles is exist get the last time of the saved candle and returns
// time: last saved candle, bool: existence of candles.
func candleLastSaveTime(symbol string) (time.Time, bool) {

	var nc uint64    // Number candles in database.
	var st string    // String time from database.
	var te time.Time // Parsed time.

	// Symbol name to lower case.
	symbol = strings.ToLower(symbol)

	// Check the existence of candles in the database.
	if e := api.database.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM fc_%s", symbol)).Scan(&nc); e != nil {
		log.Fatalln(e)
	} else if nc == 0 {
		return te, false
	}

	// Select from database last saved candle.
	if e := api.database.QueryRow(fmt.Sprintf("SELECT time FROM fc_%s ORDER BY time DESC LIMIT 1", symbol)).Scan(&st); e != nil {
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
