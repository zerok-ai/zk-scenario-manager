package zkcommon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// Struct Utils
type StructUtils interface {
	ToString(iInstance interface{}) *string
	ToReader(iString string) *strings.Reader
	ToJsonReader(iInstance interface{}) *strings.Reader
	ToJsonString(iInstance interface{}) *string
	FromJsonString(iString string, iType reflect.Type) interface{}
}

type structUtils struct {
}

func NewStructUtils() StructUtils {
	return &structUtils{}
}

func (structUtils structUtils) ToString(iInstance interface{}) *string {
	if iInstance == nil {
		return nil
	}
	return ToPtr[string](fmt.Sprint(iInstance))
}

func (structUtils structUtils) ToReader(iString string) *strings.Reader {
	iReader := strings.NewReader(iString)
	return iReader
}

func (structUtils structUtils) ToJsonReader(iInstance interface{}) *strings.Reader {
	if iInstance == nil {
		return nil
	}
	iReader := strings.NewReader(*structUtils.ToJsonString(iInstance))
	return iReader
}

func (structUtils structUtils) ToJsonString(iInstance interface{}) *string {
	if iInstance == nil {
		return nil
	}
	bytes, error := json.Marshal(iInstance)
	if error != nil {
		//TODO:Refactor
		return nil
	} else {
		iString := string(bytes)
		return &iString

	}
}

func (structUtils structUtils) FromJsonString(iString string, iType reflect.Type) interface{} {
	if iType.Kind() == reflect.Ptr {
		iType = iType.Elem()
	}
	iTypeInterface := reflect.New(iType).Interface()
	iReader := strings.NewReader(iString)
	decoder := json.NewDecoder(iReader)
	error := decoder.Decode(iTypeInterface)
	if error != nil {
		//TODO:Refactor
	}
	return iTypeInterface
}

// String Utils
type CryptoUtils struct {
}

func (cryptoUtils CryptoUtils) ToSha256(input string) [sha256.Size]byte {
	return sha256.Sum256([]byte(input))
}

func (cryptoUtils CryptoUtils) ToSha256String(prefix string, input string, suffix string) string {
	bytes := cryptoUtils.ToSha256(input)
	return prefix + hex.EncodeToString(bytes[:]) + suffix
}

// General Utils
func GetIntegerFromString(k string) (int, error) {
	return strconv.Atoi(k)
}

func ToPtr[T any](arg T) *T {
	return &arg
}

func PtrTo[T any](arg *T) T {
	if arg == nil {
		panic(errors.New("PtrTo - Passed pointer is nil"))
	}
	return *arg
}
