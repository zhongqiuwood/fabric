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

// fetchuserattributesCmd represents the fetchuserattributes command
var fetchUserAttributesCmd = &cobra.Command{
	Use:   "fetchuserattributes",
	Short: "fetchuserattributes get all attributes of one user",
	Long: `fetchuserattributes get all attributes of one user `,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("fetchuserattributes called")
		runFetchUserAttributesCmd()
	},
}

func init() {
	fetchUserAttributesCmd.Flags().StringVarP(&Id, "id", "", "", "id of user")
	fetchUserAttributesCmd.Flags().StringVarP(&Affiliation, "affiliation", "", "", "affiliation of user")
	rootCmd.AddCommand(fetchUserAttributesCmd)
}

func runFetchUserAttributesCmd() {
	if attrs, err := rpcFetchUserAllAttributes(Url, Id, Affiliation); err != nil {
		fmt.Println(err)
	} else {
		for _, attr := range attrs {
			fmt.Println("- ", attr)
		}
	}
}