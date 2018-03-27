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

// addUserCmd represents the adduser command
var addUserCmd = &cobra.Command{
	Use:   "adduser",
	Short: "add a client, peer or validator user",
	Long: `add a user, role as client, peer or validator, note param id, affiliation and password is required, role default is 1(client),
role enum 1:client  2:peer  4: validator ex:

	membership adduser --id zx --role 1 --affiliation tester --password pswofzx
	`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("adduser called")
		runAddUserCmd()
	},
}

func init() {
	addUserCmd.Flags().StringVarP(&Id, "id", "", "", "id of user")
	addUserCmd.Flags().StringVarP(&Affiliation, "affiliation", "", "", "affiliation of user")
	addUserCmd.Flags().StringVarP(&Role, "role", "1", "", "role")
	addUserCmd.Flags().StringVarP(&AttributeValue, "password", "", "", "password")
	rootCmd.AddCommand(addUserCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// adduserCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// adduserCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func runAddUserCmd() {
	if err := rpcAddUser(Url, Id, Affiliation, Password, Role); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success")
	}
}
