package auth

import "github.com/casbin/casbin"

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func New(modelPath, policyPath string) *Authorizer {
	enforcer := casbin.NewEnforcer(modelPath, policyPath)

	return &Authorizer{
		enforcer: enforcer,
	}
}

func (a *Authorizer) Authorize(subject, object, action string) error {
	if !a.enforcer.Enforce(subject, object, action) {
		return ErrPermissionDenied{
			subject: subject,
			object:  object,
			action:  action,
		}.Status().Err()
	}

	return nil
}
