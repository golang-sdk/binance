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

import "time"

type Сandle struct {
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
