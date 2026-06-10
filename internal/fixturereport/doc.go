// Package fixturereport decodes a captured FilePlugin fixture (the
// block-{H}-meta / block-{H}-data pair) through the version router and
// summarizes block + supplier activity. It is the SINGLE source for fixture
// expected.json generation (tools/fixtureextract) and verification
// (golden_walk_test.go) — generator and checker can never drift.
package fixturereport
