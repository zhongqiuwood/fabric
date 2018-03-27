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

// addAffiliationCmd represents the addaffiliation command
var addAffiliationCmd = &cobra.Command{
	Use:   "addaffiliation",
	Short: "addAffiliation add affiliation to CA server",
	Long: `addAffiliation add affiliation to CA server, param name is required, ex:

	membership-cli addaffiliation --name bank_name --parent banks
	
	this cmd add a leaf user 'banks_and_institutions.banks.bank_m'
	
	membership-cli addaffiliation --name test

	if parent not given, add a top level user 'test'

	`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("addaffiliation called")
		runAddAffiliationCmd()
	},
}

func init() {
	addAffiliationCmd.Flags().StringVarP(&Name, "name", "n", "", "name of affiliation")
	addAffiliationCmd.Flags().StringVarP(&Parent, "parent", "p", "", "parent name of affiliation")
	addAffiliationCmd.MarkFlagRequired("name")
	rootCmd.AddCommand(addAffiliationCmd)
}

func runAddAffiliationCmd() {
	if err := rpcAddAffiliation(Url, Name, Parent); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success")
	}
}