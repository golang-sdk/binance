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

type Trade struct {
	TradeId      uint64
	Price        float64
	Qty          float64
	QuoteQty     float64
	Time         time.Time
	IsBuyerMaker bool
}

type Kline struct {
	OpenTime                 time.Time
	OpenPrice                float64
	HighPrice                float64
	LowPrice                 float64
	ClosePrice               float64
	Volume                   float64
	CloseTime                time.Time
	QuoteAssetVolume         float64
	NumberOfTrades           uint64
	TakerBuyBaseAssetVolume  float64
	TakerBuyQuoteAssetVolume float64
}
