package ca

import (
	"fmt"
	"time"
	"strings"
	"sort"
	"database/sql"
	"strconv"
	"github.com/abchain/fabric/flogging"
	pb "github.com/abchain/fabric/membersrvc/protos"
	"github.com/op/go-logging"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
)

var adminLogger = logging.MustGetLogger("admin")


type Admin struct {
	*CA
	gRPCServer      *grpc.Server
}

func initializeNoTables(db *sql.DB) error {
	return nil
}

// NewAdmin sets up a new ADMIN.
func NewAdmin() *Admin {
	caadmin := &Admin{CA: NewCA("caadmin", initializeNoTables)}
	flogging.LoggingInit("ca admin")
	return caadmin
}

// ********************************* Affiliation *************************************
func (admin *Admin) FetchAllAffiliation(ctx context.Context, in *pb.Empty) (*pb.FatchAllAffiliationResp, error) {
	adminLogger.Info("gRPC Admin:FetchAllAffiliation")
	groupList, err := admin.cadb.ReadAffiliationGroups()
	if err != nil {
		adminLogger.Errorf("error from db sql %+v", err)
		return &pb.FatchAllAffiliationResp{}, err
	}
	groups := make([]string, 0)
	for _, aff := range groupList {
		// fmt.Printf("affiliation %+v \n", aff)
		// fmt.Printf("affiliation parent %+v \n", aff.parent)
		affiliation := parentsName(aff)
		groups = append(groups, affiliation)
	}
	sort.Strings(groups)
	// Return the one-time password
	return &pb.FatchAllAffiliationResp{Affiliations: groups}, nil
}

func (admin *Admin) AddAffiliation(ctx context.Context, in *pb.AddAffiliationReq) (*pb.AddAffiliationResp, error) {
	adminLogger.Debug("gRPC Admin:AddAffiliation")

	// Check the signature
	// err := ecaa.checkRegistrarSignature(in)
	// if err != nil {
	// 	return err
	// }
	name := in.Name
	parent := in.Parent
	adminLogger.Infof("affiliation %s, %s", name, parent)	
	err := admin.cadb.CheckAndAddAffiliationGroup(name, parent)
	if err != nil {
		return &pb.AddAffiliationResp{Message: "error"}, err 
	}
	// Return the one-time password
	return &pb.AddAffiliationResp{Message: "ok"}, nil
}

func (admin *Admin) DeleteAffiliation(ctx context.Context, in *pb.DeleteAffiliationReq) (*pb.Empty, error) {
	err := admin.cadb.deleteAffiliation(in.Name)
	return &pb.Empty{}, err
}


func (admin *Admin) FetchAllUsers(ctx context.Context, in *pb.FatchAllUsersReq) (*pb.FatchAllUsersResp, error) {
	adminLogger.Info("gRPC Admin:FetchAllUser")

	rule := ""

	affiliation := in.Affiliation
	// if affiliation != "" {
	// 	rule = fmt.Sprintf(" affiliation=%s ", affiliation)
	// }

	role := in.Role
	if role != "" {
		rule = fmt.Sprintf(" %s role=%s ", rule, role)
	}

	if rule != "" {
		rule = " WHERE " + rule
	}
	users, err := admin.cadb.fetchAllUsers(rule)
	if err != nil {
		adminLogger.Errorf("FetchAllUser error from db sql %+v", err)
		return &pb.FatchAllUsersResp{}, err
	}
	us := make([]string, 0)
	for _, u := range users {
		// str := attr.ToString()
		var aff string
		if affiliation != "" {
			ss := strings.Split(u.EnrollmentID, "\\")
			if len(ss) < 2 {
				continue
			}
			if ss[1] != affiliation {
				continue
			}
			aff = ss[1]
		} else {
			ss := strings.Split(u.EnrollmentID, "\\")
			if len(ss) >= 2 {
				aff = ss[1]
			}
		}
		str := fmt.Sprintf("%30s %20s %10d", u.Id, aff, u.Role)
		us = append(us, str)
	}
	sort.Strings(us)
	return &pb.FatchAllUsersResp{Users: us}, nil
}

func (admin *Admin) AddUser(ctx context.Context, in *pb.AddUserReq) (*pb.Empty, error) {
	adminLogger.Infof("gRPC Admin:AddUser %+v", in)
	role, _ := strconv.ParseInt(in.Role, 10, 32)
	token, err := admin.registerUserWithoutRegistar(in.Id, in.Affiliation, pb.Role(role), "", in.Password)
	
	if err != nil {
		adminLogger.Errorf("AddUser error from db sql %+v", err)
		return &pb.Empty{}, err
	}
	adminLogger.Info("gRPC token:", token)
	return &pb.Empty{}, nil
}

func (admin *Admin) DeleteUser(ctx context.Context, in *pb.DeleteUserReq) (*pb.Empty, error) {
	err := admin.cadb.deleteUser(in.Id)
	return &pb.Empty{}, err
}

func parentsName(aff *AffiliationGroup) string {
	str := aff.name
	if aff.parent != nil {
		parent := parentsName(aff.parent)
		str = fmt.Sprintf("%s.%s", parent, str) 
	}
	return str
}


func (admin *Admin) FetchUserAllAttributes(ctx context.Context, in *pb.FatchUserAllAttributesReq) (*pb.FatchUserAllAttributesResp, error) {
	adminLogger.Info("gRPC Admin:FetchAllAttributes")

	id := in.Id
	// affiliation := in.Affiliation
	// rule := fmt.Sprintf(" WHERE id=%s and affiliation=%s", id, affiliation)
	// rule := fmt.Sprintf(" WHERE id=%s ", id)
	// fmt.Println(rule)
	attrList, err := admin.cadb.fetchAttributes(id)
	if err != nil {
		adminLogger.Errorf("FetchAllAttributes error from db sql %+v", err)
		return &pb.FatchUserAllAttributesResp{}, err
	}
	attrs := make([]string, 0)
	for _, attr := range attrList {
		// str := attr.ToString()
		str := fmt.Sprintf("%15s %40s %s to %s", attr.GetID(), fmt.Sprintf("%s:%s", attr.attributeName, attr.attributeValue), attr.validFrom, attr.validTo)
		attrs = append(attrs, str)
	}
	sort.Strings(attrs)
	return &pb.FatchUserAllAttributesResp{Attributes: attrs}, nil
}

func (admin *Admin) AddOrUpdateUserAttribute(ctx context.Context, in *pb.AddOrUpdateUserAttributeReq) (*pb.Empty, error) {
	adminLogger.Info("gRPC Admin:AddOrUpdateUserAttribute")

	owner := &AttributeOwner{
		id: in.Id,
		affiliation: in.Affiliation,
	}
	vf := time.Now()
	if in.ValidFrom != "" {
		i, err := strconv.ParseInt(in.ValidFrom, 10, 64)
		if err != nil {
			panic(err)
		}
		vf = time.Unix(i, 0)
	}

	vt := vf.Add(time.Hour * 24 * 365)
	if in.ValidFrom != "" {
		i, err := strconv.ParseInt(in.ValidFrom, 10, 64)
		if err != nil {
			panic(err)
		}
		vt = time.Unix(i, 0)
	}
	
	
	attr := []string{in.Id, in.Affiliation, in.Name, in.Value, vf.Format(time.RFC3339), vt.Format(time.RFC3339)}
	attrPair, _ := NewAttributePair(attr, owner)

	 err := admin.cadb.addOrUpdateUserAttribute(attrPair)
	if err != nil {
		adminLogger.Errorf("AddOrUpdateUserAttribute error from db sql %+v", err)
		return &pb.Empty{}, err
	}
	return &pb.Empty{}, nil
}

func (admin *Admin) DeleteAttribute(ctx context.Context, in *pb.DeleteAttributeReq) (*pb.Empty, error) {
	err := admin.cadb.deleteAttribute(in.Id, in.AttributeName)
	return &pb.Empty{}, err
}

// Start starts the Admin.
func (admin *Admin) Start(srv *grpc.Server) {
	adminLogger.Info("Starting CA ADMIN...")
	admin.gRPCServer = srv
	pb.RegisterAdminServer(srv, admin)
	adminLogger.Info("CA Admin started.")
}

// Stop stops the CA ADMIN services.
func (admin *Admin) Stop() {
	adminLogger.Info("Stopping Admin services...")
	if admin.gRPCServer != nil {
		admin.gRPCServer.Stop()
	}
	err := admin.CA.Stop()
	if err != nil {
		adminLogger.Errorf("CA ADMIN Error stopping services: %s", err)
	} else {
		adminLogger.Info("Ca ADMIN stopped")
	}
}