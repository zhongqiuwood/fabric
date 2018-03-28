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
	"sort"
	"time"
	"errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "github.com/abchain/fabric/membersrvc/protos"
)

// ********************************* Affiliation *************************************

func rpcAffiliationsFetch(address string) ([]string, error) {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
			// fmt.Fatalf("did not connect: %v", err)
			return nil, err
	}
	defer conn.Close()
	c := pb.NewAdminClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.AffiliationsFetch(ctx, &pb.Empty{})
	if err != nil {
		// fmt.Fatalf("error when request: %v", err)
		return nil, err
	}
	strs := make([]string, 0)
	for _, aff := range r.Affiliations {
		str := fmt.Sprintf("%30s | %30s |", aff.Name, aff.Parent)
		strs = append(strs, str)
	}
	sort.Strings(strs)
	return strs, nil
}

func rpcAffiliationsAdd(address,  affiliationName, parentName string) error {
	if affiliationName == "" {
		return errors.New("Error param name empty")
	}
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		// log.Fatalf("did not connect: %v", err)
		return err
	}
	defer conn.Close()
	c := pb.NewAdminClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.AffiliationsAdd(ctx, &pb.AffiliationsAddReq{Name: affiliationName, Parent: parentName})
	if err != nil {
		// log.Fatalf("could not greet: %v", err)
		return err
	}
	return nil
}

func rpcAffiliationsDel(address, name string) error {
	if name == "" {
		return errors.New("Error param name empty")
	}
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		// log.Fatalf("rpcDeleteUser did not connect: %v", err)
		return err
	}
	defer conn.Close()
	c := pb.NewAdminClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.AffiliationsDel(ctx, &pb.AffiliationsDelReq{Name: name})
	if err != nil {
		// log.Fatalf("rpcDeleteUser error when request: %v", err)
		return err
	}

	return nil
}

// ********************************* User *************************************

func rpcUsersFetch(address, affiliation, roleNum string) ([]string, error) {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		// log.Fatalf("rpcFetchAllUsers did not connect: %v", err)
		return nil, err
	}
	defer conn.Close()
	c := pb.NewAdminClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.UsersFetch(ctx, &pb.UsersFetchReq{Affiliation: affiliation, Role: roleNum})
	if err != nil {
		// log.Fatalf("rpcFetchAllUsers error when request: %v", err)
		return nil, err
	}
	strs := make([]string, 0)
	for _, u := range r.Users {
		str := fmt.Sprintf("%30s | %30s | %10s", u.Id, u.Affiliation, u.Role)
		strs = append(strs, str)
	}
	sort.Strings(strs)
	return strs, nil
}

func rpcUsersAdd(address, id, affiliation, psw, role string) error {
	// Set up a connection to the server.
	if id == "" {
		return errors.New("Error param id empty")
	}
	if affiliation == "" {
		return errors.New("Error param affiliation empty")
	}
	if psw == "" {
		return errors.New("Error param password empty")
	}
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		// log.Fatalf("rpcAddUser did not connect: %v", err)
		return err
	}
	defer conn.Close()
	c := pb.NewAdminClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()


	_, err = c.UsersAdd(ctx, &pb.UsersAddReq{
		User: &pb.AdminUser{
			Id: id, 
			Affiliation: affiliation,
			Role: role,
			Password: psw,
		},
	})
	if err != nil {
		// log.Fatalf("rpcAddUser error when request: %v", err)
		return err
	}
	return nil
}

func rpcUsersDel(address, userId string) error {
	if userId == "" {
		return errors.New("Error param id empty")
	}
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		// log.Fatalf("rpcDeleteUser did not connect: %v", err)
		return err
	}
	defer conn.Close()
	c := pb.NewAdminClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.UsersDel(ctx, &pb.UsersDelReq{Id: userId})
	if err != nil {
		// log.Fatalf("rpcDeleteUser error when request: %v", err)
		return err
	}
	return nil
}

// ********************************* Attribute *************************************

func rpcAttributesFetch(address, id, affiliation string) ([]string, error) {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		// log.Fatalf("rpcFetchUserAllAttributes did not connect: %v", err)
		return nil, err
	}
	defer conn.Close()
	c := pb.NewAdminClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.AttributesFetch(ctx, &pb.AttributesFetchReq{Id: id})
	if err != nil {
		// log.Fatalf("rpcFetchUserAllAttributes error when request: %v", err)
		return nil, err
	}
	strs := make([]string, 0)
	for _, attr := range r.Attributes {
		str := fmt.Sprintf("%15s | %40s |    %s to %s", attr.Owner.Id, fmt.Sprintf("%s:%s", attr.Name, attr.Value), attr.ValidFrom, attr.ValidTo)
		strs = append(strs, str)
	}
	return strs, nil
}

func rpcAttributesAdd(address, id, affiliation, name, value, validFrom, validTo string) error {
	// Set up a connection to the server.
	if id == "" {
		return errors.New("Error param userid empty")
	}
	if affiliation == "" {
		return errors.New("Error param useraffiliation empty")
	}
	if name == "" {
		return errors.New("Error param name empty")
	}
	if value == "" {
		return errors.New("Error param value empty")
	}

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		// log.Fatalf("rpcAddOrUpdateUserAttribute did not connect: %v", err)
		return err
	}
	defer conn.Close()
	c := pb.NewAdminClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()


	_, err = c.AttributesAdd(ctx, &pb.AttributesAddReq{
		Attribute: &pb.AdminAttribute{
			Owner: &pb.AdminUser{
				Id: id, 
				Affiliation: affiliation,
			},
			Name: name,
			Value: value,
			ValidFrom: validFrom,
			ValidTo: validTo,
		},
	})
	if err != nil {
		// log.Fatalf("rpcFetchUserAllAttributes error when request: %v", err)
		return err
	}
	
	return nil
}

func rpcAttributesDel(address, userId, attrName string) error {
	if userId == "" {
		return errors.New("Error param userid empty")
	}
	if attrName == "" {
		return errors.New("Error param name empty")
	}
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		// log.Fatalf("rpcDeleteAttribute did not connect: %v", err)
		return err
	}
	defer conn.Close()
	c := pb.NewAdminClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.AttributesDel(ctx, &pb.AttributesDelReq{Id: userId, AttributeName: attrName})
	if err != nil {
		// log.Fatalf("rpcDeleteAttribute error when request: %v", err)
		return err
	}
	
	return nil
}
