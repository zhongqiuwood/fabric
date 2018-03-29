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
	"github.com/spf13/cobra"
)

// attributesCmd represents the attributes command
var attributesCmd = &cobra.Command{
	Use:   "attributes",
	Short: "fetch attributes, add or del a attribute",
	Long: `fetch attributes, add or del a attribute:

	'membership-cli attributes fetch'  return all attributes.
	'membership-cli attributes add --userid jim --useraffiliation bank_a --name an1 --value av1'  add one attribute to user jim.
	'membership-cli attributes del --userid jim --name an1'  del one attribute.
	
	`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("attributes called")
		runAttributesFetchCmd()
	},
}

func init() {
	attributesCmd.Flags().StringVarP(&Id, "userid", "", "", "id of user")
	attributesCmd.Flags().StringVarP(&Affiliation, "useraffiliation", "", "", "affiliation of user")
	rootCmd.AddCommand(attributesCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// attributesCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// attributesCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
