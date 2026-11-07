package monotest_test

import (
	"encoding/json"
	"fmt"
)

func jsonMarshal(v any) ([]byte, error)   { return json.Marshal(v) }
func jsonUnmarshal(d []byte, v any) error { return json.Unmarshal(d, v) }

func fmtSprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }
