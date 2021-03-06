package wfe

import (
	"os"
	"os/exec"
	"path/filepath"

	"github.com/hashicorp/go-hclog"
	"github.com/lyraproj/issue/issue"
	"github.com/lyraproj/pcore/loader"
	"github.com/lyraproj/pcore/px"
	"github.com/lyraproj/pcore/types"
	"github.com/lyraproj/pcore/yaml"
	"github.com/lyraproj/servicesdk/grpc"
	"github.com/lyraproj/servicesdk/service"
	"github.com/lyraproj/servicesdk/serviceapi"
	"github.com/lyraproj/wfe/api"
	wfs "github.com/lyraproj/wfe/service"
)

func init() {
	loader.SmartPathFactories[api.LyraLinkPath] = newLyraLinkPath
	loader.SmartPathFactories[api.GoPluginPath] = newGoPluginPath
	loader.SmartPathFactories[api.PpManifestPath] = newPpManifestPath
	loader.SmartPathFactories[api.YamlManifestPath] = newYamlManifestPath
}

func newGoPluginPath(ml px.ModuleLoader, moduleNameRelative bool) loader.SmartPath {
	return loader.NewSmartPath(`goplugins`, ``, ml, []px.Namespace{px.NsService, px.NsHandler, px.NsDefinition}, moduleNameRelative, false, instantiateGoPlugin)
}

func instantiateGoPlugin(c px.Context, l loader.ContentProvidingLoader, tn px.TypedName, sources []string) {
	loadPluginMetadata(c, l.(px.DefiningLoader), sources[0])
}

func newLyraLinkPath(ml px.ModuleLoader, moduleNameRelative bool) loader.SmartPath {
	return loader.NewSmartPath(`workflows`, `.ll`, ml, []px.Namespace{px.NsDefinition}, moduleNameRelative, false, instantiateLyraLink)
}

func instantiateLyraLink(c px.Context, l loader.ContentProvidingLoader, tn px.TypedName, sources []string) {
	dl := hclog.Default()
	lf := sources[0]
	dl.Debug("reading Lyra Link", "file", lf)
	bts := types.BinaryFromFile(lf)
	link, ok := yaml.Unmarshal(c, bts.Bytes()).(px.OrderedMap)
	if !ok {
		panic(px.Error2(issue.NewLocation(lf, 0, 0), api.LyraLinkNoMap, issue.NoArgs))
	}
	exe := ``
	if v, ok := link.Get4(`executable`); ok {
		if s, ok := v.(px.StringValue); ok {
			exe = s.String()
		}
	}
	if exe == `` {
		panic(px.Error2(issue.NewLocation(lf, 0, 0), api.LyraLinkNoExe, issue.NoArgs))
	}
	exe = os.ExpandEnv(exe)
	args := []string{}
	if v, ok := link.Get4(`arguments`); ok {
		// Accepts array of strings or a string
		if a, ok := v.(*types.Array); ok {
			args = make([]string, a.Len())
			a.EachWithIndex(func(s px.Value, i int) { args[i] = os.ExpandEnv(s.String()) })
		} else if s, ok := v.(px.StringValue); ok {
			args = []string{os.ExpandEnv(s.String())}
		}
	}
	loadPluginMetadata(c, l.(px.DefiningLoader), exe, args...)
}

func newYamlManifestPath(ml px.ModuleLoader, moduleNameRelative bool) loader.SmartPath {
	return loader.NewSmartPath(`workflows`, `.yaml`, ml, []px.Namespace{px.NsDefinition}, moduleNameRelative, false, instantiateYaml)
}

func instantiateYaml(c px.Context, l loader.ContentProvidingLoader, tn px.TypedName, sources []string) {
	// No actual difference until the plugins puppet-workflow and yaml-workflow become separated
	instantiatePp(c, l, tn, sources)
}

func newPpManifestPath(ml px.ModuleLoader, moduleNameRelative bool) loader.SmartPath {
	return loader.NewSmartPath(`workflows`, `.pp`, ml, []px.Namespace{px.NsDefinition}, moduleNameRelative, false, instantiatePp)
}

func instantiatePp(c px.Context, l loader.ContentProvidingLoader, tn px.TypedName, sources []string) {
	ppServer := wfs.GetService(c, px.NewTypedName(px.NsService, `Puppet`))
	lg := hclog.Default()
	f := sources[0]
	lg.Debug("loading manifest", "file", f)
	def := ppServer.Invoke(
		c, `Puppet::ManifestLoader`, `loadManifest`,
		types.WrapString(filepath.Dir(filepath.Dir(f))), // Search for 'workflows/../types'
		types.WrapString(f)).(serviceapi.Definition)
	sa := service.NewSubService(def)
	dl := l.(px.DefiningLoader)
	dl.SetEntry(sa.Identifier(c), px.NewLoaderEntry(sa, nil))
	loadMetadata(c, dl, ``, nil, sa)
}

func loadPluginMetadata(c px.Context, dl px.DefiningLoader, cmd string, cmdArgs ...string) {
	serviceCmd := exec.CommandContext(c, cmd, cmdArgs...)
	service, err := grpc.Load(serviceCmd, nil)
	if err != nil {
		panic(px.Error(api.FailedToLoadPlugin, issue.H{`executable`: cmd, `message`: err.Error()}))
	}

	ti := service.Identifier(c)
	lg := hclog.Default()
	lg.Debug("loaded executable", "plugin", ti)

	dl.SetEntry(ti, px.NewLoaderEntry(service, nil))

	lg.Debug("loading metadata", "plugin", cmd)
	loadMetadata(c, dl, cmd, cmdArgs, service)
	lg.Debug("done loading metadata", "plugin", cmd)
}

func loadMetadata(c px.Context, l px.DefiningLoader, cmd string, cmdArgs []string, service serviceapi.Service) {
	_, defs := service.Metadata(c)
	if len(defs) == 0 {
		return
	}

	// Register definitions
	lg := hclog.Default()
	for _, def := range defs {
		le := px.NewLoaderEntry(def, nil)
		l.SetEntry(def.Identifier(), le)
		lg.Debug("registered definition", "definition", def.Identifier().Name())

		if handlerFor, ok := def.Properties().Get4(`handlerFor`); ok {
			hn := px.NewTypedName(px.NsHandler, handlerFor.(issue.Named).Name())
			l.SetEntry(hn, le)
			lg.Debug("registered handler", "definition", def.Identifier().Name(), "handler for", hn.Name())
		}
	}
}
