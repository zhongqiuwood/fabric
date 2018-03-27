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
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "github.com/abchain/fabric/membersrvc/protos"
)

// ********************************* Affiliation *************************************

func rpcFetchAllAffiliations(address string) ([]string, error) {
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
	r, err := c.FetchAllAffiliation(ctx, &pb.Empty{})
	if err != nil {
		// fmt.Fatalf("error when request: %v", err)
		return nil, err
	}
	return r.Affiliations, nil
}

func rpcAddAffiliation(address,  affiliationName, parentName string) error {
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
	_, err = c.AddAffiliation(ctx, &pb.AddAffiliationReq{Name: affiliationName, Parent: parentName})
	if err != nil {
		// log.Fatalf("could not greet: %v", err)
		return err
	}
	return nil
}

func rpcDeleteAffiliation(address, name string) error {
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
	_, err = c.DeleteAffiliation(ctx, &pb.DeleteAffiliationReq{Name: name})
	if err != nil {
		// log.Fatalf("rpcDeleteUser error when request: %v", err)
		return err
	}

	return nil
}

// ********************************* User *************************************

func rpcFetchAllUsers(address, affiliation, roleNum string) ([]string, error) {
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
	r, err := c.FetchAllUsers(ctx, &pb.FatchAllUsersReq{Affiliation: affiliation, Role: roleNum})
	if err != nil {
		// log.Fatalf("rpcFetchAllUsers error when request: %v", err)
		return nil, err
	}
	
	return r.Users, nil
}

func rpcAddUser(address, id, affiliation, psw, role string) error {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		// log.Fatalf("rpcAddUser did not connect: %v", err)
		return err
	}
	defer conn.Close()
	c := pb.NewAdminClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()


	_, err = c.AddUser(ctx, &pb.AddUserReq{
		Id: id, 
		Affiliation: affiliation,
		Role: role,
		Password: psw,
	})
	if err != nil {
		// log.Fatalf("rpcAddUser error when request: %v", err)
		return err
	}
	return nil
}

func rpcDeleteUser(address, userId string) error {
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
	_, err = c.DeleteUser(ctx, &pb.DeleteUserReq{Id: userId})
	if err != nil {
		// log.Fatalf("rpcDeleteUser error when request: %v", err)
		return err
	}
	return nil
}

// ********************************* Attribute *************************************

func rpcFetchUserAllAttributes(address, id, affiliation string) ([]string, error) {
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
	r, err := c.FetchUserAllAttributes(ctx, &pb.FatchUserAllAttributesReq{Id: id, Affiliation: affiliation})
	if err != nil {
		// log.Fatalf("rpcFetchUserAllAttributes error when request: %v", err)
		return nil, err
	}
	
	return r.Attributes, nil
}

func rpcAddOrUpdateUserAttribute(address, id, affiliation, name, value, validFrom, validTo string) error {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		// log.Fatalf("rpcAddOrUpdateUserAttribute did not connect: %v", err)
		return err
	}
	defer conn.Close()
	c := pb.NewAdminClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()


	_, err = c.AddOrUpdateUserAttribute(ctx, &pb.AddOrUpdateUserAttributeReq{
		Id: id, 
		Affiliation: affiliation,
		Name: name,
		Value: value,
		ValidFrom: validFrom,
		ValidTo: validTo,
	})
	if err != nil {
		// log.Fatalf("rpcFetchUserAllAttributes error when request: %v", err)
		return err
	}
	
	return nil
}

func rpcDeleteAttribute(address, userId, attrName string) error {
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
	_, err = c.DeleteAttribute(ctx, &pb.DeleteAttributeReq{Id: userId, AttributeName: attrName})
	if err != nil {
		// log.Fatalf("rpcDeleteAttribute error when request: %v", err)
		return err
	}
	
	return nil
}
