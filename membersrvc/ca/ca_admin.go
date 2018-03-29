package ca

import (
	"fmt"
	"time"
	"strings"
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
	caadmin := &Admin{CA: NewCA("caadmin", initializeNoTables, false)}
	flogging.LoggingInit("ca admin")
	return caadmin
}

// ********************************* Affiliation *************************************

func (admin *Admin) AffiliationsFetch(ctx context.Context, in *pb.Empty) (*pb.AffiliationsFetchResp, error) {
	adminLogger.Info("gRPC Admin:FetchAllAffiliation")
	groupList, err := admin.cadb.ReadAffiliationGroups()
	if err != nil {
		adminLogger.Errorf("error from db sql %+v", err)
		return &pb.AffiliationsFetchResp{}, err
	}
	// groups := make([]string, 0)
	groups := make([]*pb.AdminAffiliation, 0)
	for _, aff := range groupList {
		// affiliation := parentsName(aff)
		parentName := ""
		if aff.parent != nil {
			parentName = aff.parent.name
		}
		groups = append(groups, &pb.AdminAffiliation{Name: aff.name, Parent: parentName})
	}
	// sort.Strings(groups)
	return &pb.AffiliationsFetchResp{Affiliations: groups}, nil
}

func (admin *Admin) AffiliationsAdd(ctx context.Context, in *pb.AffiliationsAddReq) (*pb.Empty, error) {
	adminLogger.Debug("gRPC Admin:AddAffiliation")
	name := in.Name
	parent := in.Parent
	adminLogger.Infof("affiliation %s, %s", name, parent)	
	err := admin.cadb.CheckAndAddAffiliationGroup(name, parent)
	if err != nil {
		return &pb.Empty{}, err 
	}
	return &pb.Empty{}, nil
}

func (admin *Admin) AffiliationsDel(ctx context.Context, in *pb.AffiliationsDelReq) (*pb.Empty, error) {
	err := admin.cadb.deleteAffiliation(in.Name)
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

// ********************************* User *************************************

func (admin *Admin) UsersFetch(ctx context.Context, in *pb.UsersFetchReq) (*pb.UsersFetchResp, error) {
	adminLogger.Info("gRPC Admin:UsersFetch")

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
		adminLogger.Errorf("UsersFetch error from db sql %+v", err)
		return &pb.UsersFetchResp{}, err
	}
	us := make([]*pb.AdminUser, 0)
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
		// str := fmt.Sprintf("%30s %20s %10d", u.Id, aff, u.Role)
		u := &pb.AdminUser{
			Id: u.Id,
			Affiliation: aff,
			Role: strconv.FormatInt(int64(u.Role), 10),
		}
		us = append(us, u)
	}
	// sort.Strings(us)
	return &pb.UsersFetchResp{Users: us}, nil
}

func (admin *Admin) UsersAdd(ctx context.Context, in *pb.UsersAddReq) (*pb.Empty, error) {
	adminLogger.Infof("gRPC Admin:UsersAdd %+v", in)
	role, _ := strconv.ParseInt(in.User.Role, 10, 32)
	token, err := admin.registerUserWithoutRegistar(in.User.Id, in.User.Affiliation, pb.Role(role), "", in.User.Password)
	
	if err != nil {
		adminLogger.Errorf("UsersAdd error from db sql %+v", err)
		return &pb.Empty{}, err
	}
	adminLogger.Info("gRPC token:", token)
	return &pb.Empty{}, nil
}

func (admin *Admin) UsersDel(ctx context.Context, in *pb.UsersDelReq) (*pb.Empty, error) {
	err := admin.cadb.deleteUser(in.Id)
	return &pb.Empty{}, err
}

// ********************************* Attribute *************************************

func (admin *Admin) AttributesFetch(ctx context.Context, in *pb.AttributesFetchReq) (*pb.AttributesFetchResp, error) {
	adminLogger.Info("gRPC Admin:AttributesFetch")
	
	attrList, err := admin.cadb.fetchAttributes(in.Id)
	if err != nil {
		adminLogger.Errorf("AttributesFetch error from db sql %+v", err)
		return &pb.AttributesFetchResp{}, err
	}
	attrs := make([]*pb.AdminAttribute, 0)
	for _, attr := range attrList {
		// str := fmt.Sprintf("%15s %40s %s to %s", attr.GetID(), fmt.Sprintf("%s:%s", attr.attributeName, attr.attributeValue), attr.validFrom, attr.validTo)
		a := &pb.AdminAttribute{
			Owner: &pb.AdminUser{
				Id: attr.GetID(),
				Affiliation: attr.GetAffiliation(),
			},
			Name: attr.attributeName,
			Value: string(attr.attributeValue),
			ValidFrom: attr.validFrom.String(),
			ValidTo: attr.validTo.String(),
		}
		attrs = append(attrs, a)
	}
	// sort.Strings(attrs)
	return &pb.AttributesFetchResp{Attributes: attrs}, nil
}

func (admin *Admin) AttributesAdd(ctx context.Context, in *pb.AttributesAddReq) (*pb.Empty, error) {
	adminLogger.Info("gRPC Admin:AttributesAdd")
	fmt.Println(in)
	owner := &AttributeOwner{
		id: in.Attribute.Owner.Id,
		affiliation: in.Attribute.Owner.Affiliation,
	}
	vf := time.Now()
	if in.Attribute != nil && in.Attribute.ValidFrom != "" {
		i, err := strconv.ParseInt(in.Attribute.ValidFrom, 10, 64)
		if err != nil {
			panic(err)
		}
		vf = time.Unix(i, 0)
	}

	vt := vf.Add(time.Hour * 24 * 365)
	if in.Attribute != nil && in.Attribute.ValidFrom != "" {
		i, err := strconv.ParseInt(in.Attribute.ValidFrom, 10, 64)
		if err != nil {
			panic(err)
		}
		vt = time.Unix(i, 0)
	}
	
	
	attr := []string{in.Attribute.Owner.Id, in.Attribute.Owner.Affiliation, in.Attribute.Name, in.Attribute.Value, vf.Format(time.RFC3339), vt.Format(time.RFC3339)}
	attrPair, _ := NewAttributePair(attr, owner)

	 err := admin.cadb.addOrUpdateUserAttribute(attrPair)
	if err != nil {
		adminLogger.Errorf("AttributesAdd error from db sql %+v", err)
		return &pb.Empty{}, err
	}
	return &pb.Empty{}, nil
}

func (admin *Admin) AttributesDel(ctx context.Context, in *pb.AttributesDelReq) (*pb.Empty, error) {
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