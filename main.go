/*
Copyright © 2022 Speedscale, Inc. <matt@speedscale.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"fmt"
	"net/url"
	"os"

	"github.com/speedscale/pixie-to-curl/export"
)

func main() {
	validateArgs()
	args := os.Args
	r := export.NewPixieToCurl(args[1], args[2], args[3], args[4])
	if err := r.Run(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func validateArgs() {
	args := os.Args
	stop := false
	if len(args) < 2 {
		fmt.Println("missing api_token parameter")
		stop = true
	}

	if len(args) < 3 {
		fmt.Println("missing cluster_id")
		stop = true
	}

	if len(args) < 4 {
		fmt.Println("missing destination_filter")
		stop = true
	}

	if len(args) < 5 {
		fmt.Println("missing base URL")

		_, err := url.Parse(args[4])
		if err != nil {
			fmt.Printf("base url must be a valid URL (http://example.com:8080)")
			stop = true
		}
		stop = true
	}

	if stop {
		fmt.Println(`pixie-to-curl exports a set of HTTP traffic using the Pixie API and turns it into a series of curl commands.

In other words, turns Pixie into a traffic replay mechanism powered by eBPF and curl commands

Usage:
	pixie-to-curl api_token cluster_id destination_filter base_url

api_token - your unique API key generated in the Pixie UI or CLI
cluster_id - UID of the cluster you want to record from
destination_filter - name of the pod receiving traffic (usually the service you want to test)
base_url - prefix for each HTTP curl command (it isn't usually the same between environments)

Examples:
	pixie-to-curl px-api-<UID> 123456-1234-1234-1234-12312331 podtato http://podtato-head-entry.default.svc.cluster.local
	pixie-to-curl px-api-<UID> 123456-1234-1234-1234-12312331 payment http://payment.default.svc.cluster.local`)

		os.Exit(1)
	}
}
