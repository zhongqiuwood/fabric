// Copyright blackpai.com. 2018 All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// fetchallaffiliationsCmd represents the fetchallaffiliations command
var affiliationsFetchCmd = &cobra.Command{
	Use:   "fetch",
	Short: "get all affiliations from CA server",
	Long: `get all affiliations from CA server`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("fetchallaffiliations called")
		runAffiliationsFetchCmd()
	},
}

func init() {
	affiliationsCmd.AddCommand(affiliationsFetchCmd)
	// rootCmd.AddCommand(fetchAllAffiliationsCmd)
}

func runAffiliationsFetchCmd() {
	if affs, err := rpcAffiliationsFetch(Url); err != nil {
		fmt.Println("")
		fmt.Println(err)
		fmt.Println("")
	} else {
		
		fmt.Println("--------------------------------------------------------------------------------------------------------------------------------------")
		fmt.Println("    ", fmt.Sprintf("%30s %32s", "name |", "parent.name |"))
		fmt.Println("--------------------------------------------------------------------------------------------------------------------------------------")
		
		for _, aff := range affs {
			fmt.Println("- ", aff)
		}
		fmt.Println("")
		fmt.Printf("> %d affiliations received. \n", len(affs))
		fmt.Println("")
	}
}
