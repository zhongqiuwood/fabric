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
	// "fmt"

	"github.com/spf13/cobra"
)

// affiliationCmd represents the affiliation command
var affiliationsCmd = &cobra.Command{
	Use:   "affiliations",
	Short: "affiliaitons fecth, add or delete affiliation",
	Long: `affiliaitons fecth, add or delete affiliation:

	'membership-cli affiliations'  return all affiliations.
	'membership-cli affiliations add --name aff1'  add affiliation aff1.
	'membership-cli affiliations del --name aff1'  del affiliaiton aff1.
	
	`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("affiliation called")
		runAffiliationsFetchCmd()
	},
}

func init() {
	rootCmd.AddCommand(affiliationsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// affiliationCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// affiliationCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
