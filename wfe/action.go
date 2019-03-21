package wfe

import (
	"github.com/hashicorp/go-hclog"
	"github.com/lyraproj/pcore/px"
	"github.com/lyraproj/servicesdk/serviceapi"
	"github.com/lyraproj/wfe/api"
)

type action struct {
	Activity
	api px.ObjectType
}

/* TODO: Add type check using expectedType
var ioType = types.NewHashType(types.DefaultStringType(), types.DefaultRichDataType(), nil)
var expectedType = types.NewCallableType(
	types.NewTupleType([]px.Type{ioType}, nil), ioType, nil)
*/

func Action(def serviceapi.Definition) api.Activity {
	a := &action{}
	a.init(def)
	if i, ok := def.Properties().Get4(`interface`); ok {
		a.api = i.(px.ObjectType)
	}
	return a
}

func (s *action) Run(ctx px.Context, input px.OrderedMap) px.OrderedMap {
	service := s.GetService(ctx)
	hclog.Default().Debug(`executing action`, `name`, s.name)
	result := service.Invoke(ctx, s.Name(), `do`, input)
	if m, ok := result.(px.OrderedMap); ok {
		return m
	}
	panic(result.String())
}

func (s *action) Label() string {
	return ActivityLabel(s)
}

func (s *action) Style() string {
	return `action`
}
