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

// delattrCmd represents the delattr command
var attributesDelCmd = &cobra.Command{
	Use:   "del",
	Short: "del a attribute of one user",
	Long: `del a attribute of one user:

	'membership-cli attributes del --userid jim --attributename n1`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("delattr called")
		runAttributesDelCmd()
	},
}

func init() {
	attributesDelCmd.Flags().StringVarP(&Id, "userid", "", "", "user id of user")
	attributesDelCmd.Flags().StringVarP(&AttributeName, "name", "", "", "attribute name of user")
	attributesCmd.AddCommand(attributesDelCmd)
}

func runAttributesDelCmd() {
	if err := rpcAttributesDel(Url, Id, AttributeName); err != nil {
		fmt.Println("")
		fmt.Println(err)
		fmt.Println("")
	} else {
		fmt.Println("")
		fmt.Println("success")
		fmt.Println("")
	}
}