// Package version records versioning information about this module.
package version

import "fmt"

const (
	Major      = 0
	Minor      = 1
	Patch      = 0
	PreRelease = "devel"
)

// String formats the version in semver format, see semver.org
func String() string {
	v := fmt.Sprintf("%d.%d.%d", Major, Minor, Patch)
	if PreRelease != "" {
		v += "-" + PreRelease
	}
	return v
}
