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
	"log"
	"strconv"
)

func convertAnyToInt(value any) int64 {

	var r int64

	if b, e := json.Marshal(value); e == nil {

		if e := json.Unmarshal(b, &r); e != nil {
			log.Fatalln(r)
		}

	} else {
		log.Fatal(e)
	}

	return r
}

func convertAnyToUint(value any) uint64 {

	var r uint64

	if b, e := json.Marshal(value); e == nil {

		if e := json.Unmarshal(b, &r); e != nil {
			log.Fatalln(r)
		}

	} else {
		log.Fatal(e)
	}

	return r
}

func convertAnyToString(value any) string {

	var r string

	if b, e := json.Marshal(value); e == nil {

		if e := json.Unmarshal(b, &r); e != nil {
			log.Fatalln(r)
		}

	} else {
		log.Fatalln(e)
	}

	return r
}

func convertStringToFloat(value string) float64 {

	if f, e := strconv.ParseFloat(convertAnyToString(value), 64); e == nil {
		return f
	} else {
		log.Fatalln(e)
	}
	return 0
}
