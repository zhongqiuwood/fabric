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

// fetchallusersCmd represents the fetchallusers command
var usersFetchCmd = &cobra.Command{
	Use:   "fetch",
	Short: "fetch user by role or affiliation",
	Long: `fetch user by role or affiliation.`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("fetchallusers called")
		runUsersFetchCmd()
	},
}

func init() {
	usersFetchCmd.Flags().StringVarP(&Affiliation, "affiliation", "", "", "affiliation of user")
	usersFetchCmd.Flags().StringVarP(&Role, "role", "", "", "role of user to fetch")
	usersCmd.AddCommand(usersFetchCmd)
}

func runUsersFetchCmd() {
	if users, err := rpcUsersFetch(Url, Affiliation, Role); err != nil {
		fmt.Println("")
		fmt.Println(err)
		fmt.Println("")
	} else {
		str := fmt.Sprintf("%30s %32s %10s", "id |", "affiliation |", "role |")
		fmt.Println("--------------------------------------------------------------------------------------------------------------------------------------")
		fmt.Println("    ", str)
		fmt.Println("--------------------------------------------------------------------------------------------------------------------------------------")
		
		for _, u := range users {
			fmt.Println("- ", u)
		}
		fmt.Println("")
		fmt.Printf("> %d users received. \n", len(users))
		fmt.Println("")
	}
}