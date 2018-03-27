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

// adduserattrbuteCmd represents the adduserattrbute command
var addUserAttributeCmd = &cobra.Command{
	Use:   "adduserattribute",
	Short: "adduserattribute add attribute to user",
	Long: `adduserattribute add attribute to user, param userid, useraffiliation, attributename 
	and attributevalue are required. option　validfrom and validto is timestemp, validfrom default 
	the timestemp of now, and validto is the timestemp of 1 year from now, ex:
	
	adduserattribute  --userid jim --useraffiliation bank_a --attributename an1 --attributevalue av1
	`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("adduserattrrbute called")
		runAddUserAttrrbuteCmd()
	},
}

func init() {
	addUserAttributeCmd.Flags().StringVarP(&Id, "userid", "", "", "id of user")
	addUserAttributeCmd.Flags().StringVarP(&Affiliation, "useraffiliation", "", "", "affiliation of user")
	addUserAttributeCmd.Flags().StringVarP(&AttributeName, "attributename", "", "", "AttributeName")
	addUserAttributeCmd.Flags().StringVarP(&AttributeValue, "attributevalue", "", "", "AttributeValue")
	addUserAttributeCmd.Flags().StringVarP(&ValidFrom, "validfrom", "", "", "ValidFrom")
	addUserAttributeCmd.Flags().StringVarP(&ValidTo, "validto", "", "", "ValidTo")
	rootCmd.AddCommand(addUserAttributeCmd)
}


func runAddUserAttrrbuteCmd() {
	if err := rpcAddOrUpdateUserAttribute(Url, Id, Affiliation, AttributeName, AttributeValue, ValidFrom, ValidTo); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success")
	}
}
