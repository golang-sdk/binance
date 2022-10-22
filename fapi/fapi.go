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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

var client = &http.Client{Timeout: time.Minute}

// Universal getting json from Binance perpetual future API.
func requestEndpoint(key, endpoint string, queries *map[string]string, response any) (time.Time, int64) {

	// Create request.
	req, err := http.NewRequest("GET", fmt.Sprintf("https://fapi.binance.com/fapi/v1/%s", endpoint), nil)
	if err != nil {
		log.Fatalf("Error %s when create new request", err)
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
	req.Header.Add("X-MBX-APIKEY", key)

	// Execute request.
	res, err := client.Do(req)
	if err != nil {
		log.Fatalf("Error %s when execute request", err)
	}
	defer res.Body.Close()

	// Detect ban.
	if res.StatusCode == 429 || res.StatusCode == 418 {
		log.Fatalf("Error status code %d", res.StatusCode)
	}

	// Detect content type.
	if res.Header.Get("Content-Type") != "application/json" {
		log.Fatalln("The request returned an unexpected content type")
	}

	// Server response time.
	dat, err := time.Parse(time.RFC1123, res.Header.Get("Date"))
	if err != nil {
		log.Fatalf("error %s when parse response header date", err)
	}

	// Weight responce.
	weg, err := strconv.ParseInt(res.Header.Get("X-MBX-USED-WEIGHT-1M"), 10, 64)
	if err != nil {
		log.Fatalf("error %s when getting weight", err)
	}

	// Body from response.
	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("error %s when read body request", err)
	}

	// Parse json.
	if err := json.Unmarshal(body, response); err != nil {
		log.Fatalf("error %s when getting trades and json marshal", err)
	}

	return dat, weg
}

// Returns trades, server response time and weight 1m
func getHistoricalTrades(key, symbol string, fromId int64) ([]Trade, time.Time, int64) {

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
	dat, weg := requestEndpoint(key, "historicalTrades", &que, &jtr)

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
			log.Fatalf("error %s when convert type price", e)
		}

		// Qty convert string to float.
		if f, e := strconv.ParseFloat(t.Qty, 64); e == nil {
			trade.Qty = f
		} else {
			log.Fatalf("error %s when convert type qty", e)
		}

		// Quote qty convert string to float.
		if f, e := strconv.ParseFloat(t.QuoteQty, 64); e == nil {
			trade.QuoteQty = f
		} else {
			log.Fatalf("error %s when convert type quote qty", e)
		}

		tra = append(tra, trade)
	}

	return tra, dat, weg
}
