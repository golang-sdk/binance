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

type Kline struct {
	OT time.Time // Kline open time.
	OP float64   // Open price.
	HP float64   // High price.
	LP float64   // Low price.
	CP float64   // Close price.
	VE float64   // Volume.
	CT time.Time // Kline Close time.
	QA float64   // Quote asset volume.
	NT uint64    // Number of trades.
	TB float64   // Taker buy base asset volume.
	TQ float64   // Taker buy quote asset volume.
}
