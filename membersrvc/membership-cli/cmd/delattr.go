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
var deleteAttributeCmd = &cobra.Command{
	Use:   "delattr",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("delattr called")
		runDeleteAttributeCmd()
	},
}

func init() {
	deleteAttributeCmd.Flags().StringVarP(&Id, "userid", "", "", "user id of user")
	deleteAttributeCmd.Flags().StringVarP(&AttributeName, "attrname", "", "", "attribute name of user")
	rootCmd.AddCommand(deleteAttributeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// delattrCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// delattrCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func runDeleteAttributeCmd() {
	if err := rpcDeleteAttribute(Url, Id, AttributeName); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success")
	}
}