package service

import (
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
	"golang.org/x/crypto/bcrypt"
	"net/http"
	"time"
)

type AuthService interface {
	User(name string) *cluster.UserInfo
	Users() []cluster.UserInfo
	CreateUser(user cluster.UserInfo) error
	UpdateUser(user cluster.UserInfo) error
	DeleteUser(name string) error
	SetPrivilege(name, database string, p influxql.Privilege) error
	RemovePrivilege(name, database string) error
	HasAdmin() bool
}

// authenticate verifies that the user exists and that the password is correct.
// If the user does not exist, and an admin has not yet been set, it will return
// nil for the user and nil for the error as it is a valid state.
func authenticate(r *http.Request, authService AuthService) (*cluster.UserInfo, error) {
	if authService.HasAdmin() {
		if username, password, ok := r.BasicAuth(); ok {
			user := authService.User(username)
			if user == nil {
				return nil, meta.ErrAuthenticate
			}
			if bcrypt.CompareHashAndPassword([]byte(user.Hash), []byte(password)) != nil {
				return nil, meta.ErrAuthenticate
			}
			return user, nil
		}
		return nil, meta.ErrAuthenticate
	}
	return nil, nil
}

func isAllowed(privileges influxql.ExecutionPrivileges, user cluster.UserInfo, db string) bool {
	for _, p := range privileges {
		if p.Admin && !user.Admin {
			return false
		}
		if p.Name != "" && db == db && !user.AuthorizeDatabase(p.Privilege, db) {
			return false
		}
	}
	return true
}

type PersistentAuthService struct {
	storage cluster.AuthStorage
	auth    *cluster.AuthData
	dirty   bool
}

func (service *PersistentAuthService) HasAdmin() bool {
	if service.auth == nil {
		return false
	}
	for _, user := range service.auth.Users {
		if user.Admin {
			return true
		}
	}
	return false
}

func NewPersistentAuthService(storage cluster.AuthStorage) *PersistentAuthService {
	return &PersistentAuthService{storage: storage, auth: cluster.NewAuthData(), dirty: false}
}

func (service *PersistentAuthService) Save() error {
	if !service.dirty || service.auth == nil {
		return nil
	}
	err := service.storage.Save(service.auth)
	if err == nil {
		service.dirty = false
		service.refresh()
	}
	return err
}

func (service *PersistentAuthService) User(name string) *cluster.UserInfo {
	if service.auth == nil {
		return nil
	}
	for _, user := range service.auth.Users {
		if user.Name == name {
			return &user
		}
	}
	return nil
}

func (service *PersistentAuthService) Users() []cluster.UserInfo {
	if service.auth == nil {
		return []cluster.UserInfo{}
	}
	return service.auth.Users
}

func (service *PersistentAuthService) CreateUser(user cluster.UserInfo) error {
	if user.Name == "" {
		return meta.ErrUsernameRequired
	} else if service.User(user.Name) != nil {
		return meta.ErrUserExists
	}
	service.auth.Users = append(service.auth.Users, user)
	service.dirty = true
	return nil
}

func (service *PersistentAuthService) UpdateUser(user cluster.UserInfo) error {
	if user.Name == "" {
		return meta.ErrUsernameRequired
	}
	for i, u := range service.auth.Users {
		if u.Name == user.Name {
			service.auth.Users[i] = user
			service.dirty = true
			return nil
		}
	}
	return meta.ErrUserNotFound
}

func (service *PersistentAuthService) DeleteUser(name string) error {
	for i, u := range service.auth.Users {
		if u.Name == name {
			service.auth.Users = append(service.auth.Users[:i], service.auth.Users[i:]...)
			service.dirty = true
			return nil
		}
	}
	return meta.ErrUserNotFound
}

func (service *PersistentAuthService) SetPrivilege(name, database string, p influxql.Privilege) error {
	user := service.User(name)
	if user == nil {
		return meta.ErrUserNotFound
	}

	// TODO Consider keeping track of all database created
	//if service.Database(database) == nil {
	//	return influxdb.ErrDatabaseNotFound(database)
	//}

	if user.Privileges == nil {
		user.Privileges = make(map[string]influxql.Privilege)
	}
	service.dirty = user.Privileges[database] != p
	user.Privileges[database] = p

	return nil
}

func (service *PersistentAuthService) RemovePrivilege(name, database string) error {
	user := service.User(name)
	if user == nil {
		return meta.ErrUserNotFound
	}
	if user.Privileges != nil {
		delete(user.Privileges, database)
		service.dirty = true
	}
	return nil
}

func (service *PersistentAuthService) refresh() error {
	auth, err := service.storage.Get()
	if err != nil {
		return err
	}
	if auth != nil {
		service.auth = auth
	}
	return nil
}

func (service *PersistentAuthService) Sync() {
	ch := service.storage.Watch()
	ticker := time.NewTicker(10 * time.Second)
	service.refresh()
	for {
		select {
		case auth := <-ch:
			if !service.dirty {
				*service.auth = auth
			}
		case <-ticker.C:
			if !service.dirty {
				service.refresh()
			}
		}
	}
}

func HandleAuthStatement(stmt influxql.Statement, authService AuthService) (results []Result, err error) {
	switch s := stmt.(type) {
	case
		*influxql.CreateUserStatement:
		err = authService.CreateUser(cluster.NewUser(s.Name, cluster.HashUserPassword(s.Password), s.Admin))
	case
		*influxql.DropUserStatement:
		err = authService.DeleteUser(s.Name)
	case
		*influxql.GrantStatement:
		err = authService.SetPrivilege(s.User, s.DefaultDatabase(), s.Privilege)
	case
		*influxql.GrantAdminStatement:
		err = updateUser(s.User, authService, func(u *cluster.UserInfo) {
			u.Admin = true
		})
	case
		*influxql.RevokeStatement:
		err = authService.RemovePrivilege(s.User, s.DefaultDatabase())
	case
		*influxql.RevokeAdminStatement:
		err = updateUser(s.User, authService, func(u *cluster.UserInfo) {
			u.Admin = false
		})
	case
		*influxql.SetPasswordUserStatement:
		err = updateUser(s.Name, authService, func(u *cluster.UserInfo) {
			u.Hash = cluster.HashUserPassword(s.Password)
		})
	case
		*influxql.ShowUsersStatement:
		var values [][]interface{}
		for _, user := range authService.Users() {
			values = append(values, []interface{}{user.Name, user.Admin})
		}
		results = createListResults("", []string{"user", "admin"}, values)
	}
	return
}

func updateUser(user string, authService AuthService, updater func(info *cluster.UserInfo)) error {
	u := authService.User(user)
	if u == nil {
		return meta.ErrUserNotFound
	} else {
		updater(u)
		return authService.UpdateUser(*u)
	}
}
