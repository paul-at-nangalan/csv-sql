package function

import "fmt"

type Invalid struct{
	reason string
}

func (p Invalid) Error() string {
	return p.reason
}

func NewInvalid(reason string)Invalid{
	return Invalid{
		reason: reason,
	}
}

type InvalidClause struct{
	Invalid
}

func NewInvalidClause(clause string, reason string)InvalidClause{
	details := "Invalid clause: " + clause + ": " + reason
	return InvalidClause{
		Invalid{
			reason: details,
		},
	}
}

type InvalidFieldName struct{
	Invalid
}

func NewInvalidFieldName(fieldname string)InvalidFieldName {
	details := "Invalid field name: " + fieldname
	return InvalidFieldName{
		Invalid{
			reason: details,
		},
	}
}

type InvalidValue struct{
	Invalid
}

func NewInvalidValue(val interface{})InvalidValue {
	details := fmt.Sprint("Invalid float value: ", val)
	return InvalidValue{
		Invalid{
			reason: details,
		},
	}
}
type InvalidString struct{
	Invalid
}

func NewInvalidString(val interface{})InvalidString {
	details := fmt.Sprint("Invalid string value, (to compare strings and float values, compare them as floats): ", val)
	return InvalidString{
		Invalid{
			reason: details,
		},
	}
}
