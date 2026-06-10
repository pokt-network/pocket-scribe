package main

import (
	"bytes"
	"testing"
)

const in = `package shared

func init() {
	proto.RegisterType((*Supplier)(nil), "pocket.shared.Supplier")
	proto.RegisterEnum("pocket.shared.RPCType", RPCType_name, RPCType_value)
	proto.RegisterMapType((map[string]string)(nil), "pocket.shared.M.Entry")
}

func init() { proto.RegisterFile("pocket/shared/supplier.proto", fileDescriptor_aabb) }

func init() {
	proto.RegisterType((*X)(nil), "pocket.shared.X")
	somethingElse()
}

func keep() {}
`

const want = `package shared

func init() {
	somethingElse()
}

func keep() {}
`

func TestTransformStripsRegistrationsOnly(t *testing.T) {
	got := transform([]byte(in))
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("transform mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestTransformIdempotent(t *testing.T) {
	once := transform([]byte(in))
	twice := transform(once)
	if !bytes.Equal(once, twice) {
		t.Fatal("transform is not idempotent")
	}
}
