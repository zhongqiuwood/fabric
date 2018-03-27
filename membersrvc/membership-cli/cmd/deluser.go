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

// deluserCmd represents the deluser command
var delUserCmd = &cobra.Command{
	Use:   "deluser",
	Short: "deluser a user from ca server by id",
	Long: `deluser a user from ca server by id ex:
	
	membership-cli deluser --id bob`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("deluser called")
		runDeleteUserCmd();
	},
}

func init() {
	delUserCmd.Flags().StringVarP(&Id, "id", "", "", "user id of user")
	rootCmd.AddCommand(delUserCmd)
}

func runDeleteUserCmd() {
	if err := rpcDeleteUser(Url, Id); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success")
	}
}