package conformance

import "google.golang.org/protobuf/reflect/protoreflect"

func clearUnknown(m protoreflect.Message) {
	m.SetUnknown(nil)
	m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if f.Kind() == protoreflect.MessageKind {
			if f.IsList() {
				l := v.List()
				for i := range l.Len() {
					clearUnknown(l.Get(i).Message())
				}
			} else {
				clearUnknown(v.Message())
			}
		}
		return true
	})
}
