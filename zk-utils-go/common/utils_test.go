package zkcommon

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"reflect"
	"testing"
)

type inputStructInner struct {
	StringKey  string `json:"stringKey"`
	IntegerKey int    `json:"integerKey"`
}

type inputStruct struct {
	StringKey string           `json:"stringKey"`
	StructKey inputStructInner `json:"structKey"`
}

type StructUtilsTestSuite struct {
	suite.Suite
	structUtils           StructUtils
	inputStruct           inputStruct
	inputStructFmtString  string
	inputStructJsonString string
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestStructUtilsTestSuite(t *testing.T) {
	suite.Run(t, new(StructUtilsTestSuite))
}

func (structUtilsTestSuite *StructUtilsTestSuite) SetupTest() {
	structUtilsTestSuite.structUtils = NewStructUtils()
	structUtilsTestSuite.inputStructFmtString = "{HelloFrom-inputStruct {HelloFrom-inputStructInner 55}}"
	structUtilsTestSuite.inputStructJsonString = "{\"stringKey\":\"HelloFrom-inputStruct\",\"structKey\":{\"stringKey\":\"HelloFrom-inputStructInner\",\"integerKey\":55}}"
	structUtilsTestSuite.inputStruct = inputStruct{
		StringKey: "HelloFrom-inputStruct",
		StructKey: inputStructInner{
			StringKey:  "HelloFrom-inputStructInner",
			IntegerKey: 55,
		},
	}
}

// Test_<StructName>_<Case-Description>_<MethodName>_<Success/Failure>
func (structUtilsTestSuite *StructUtilsTestSuite) Test_StructUtils_NormalInput_ToString_Success() {
	output := structUtilsTestSuite.structUtils.ToString(structUtilsTestSuite.inputStruct)
	assert.Equal(structUtilsTestSuite.T(), structUtilsTestSuite.inputStructFmtString, *output)
}

func (structUtilsTestSuite *StructUtilsTestSuite) Test_StructUtils_NilInput_ToString_Success() {
	output := structUtilsTestSuite.structUtils.ToString(nil)
	assert.Nil(structUtilsTestSuite.T(), output)
}

func (structUtilsTestSuite *StructUtilsTestSuite) Test_StructUtils_EmptyInput_ToString_Success() {
	output := structUtilsTestSuite.structUtils.ToString(inputStruct{})
	assert.Equal(structUtilsTestSuite.T(), "{ { 0}}", *output)
}

func (structUtilsTestSuite *StructUtilsTestSuite) Test_StructUtils_NormalInput_ToReader_Success() {
	output := structUtilsTestSuite.structUtils.ToReader(structUtilsTestSuite.inputStructFmtString)
	buffer := make([]byte, len(structUtilsTestSuite.inputStructFmtString))
	_, err := output.Read(buffer)
	assert.Nil(structUtilsTestSuite.T(), err)
	assert.Equal(structUtilsTestSuite.T(), structUtilsTestSuite.inputStructFmtString, string(buffer))
}

func (structUtilsTestSuite *StructUtilsTestSuite) Test_StructUtils_NormalInput_ToJsonReader_Success() {
	output := structUtilsTestSuite.structUtils.ToJsonReader(structUtilsTestSuite.inputStruct)
	buffer := make([]byte, len(structUtilsTestSuite.inputStructJsonString))
	_, err := output.Read(buffer)
	assert.Nil(structUtilsTestSuite.T(), err)
	assert.Equal(structUtilsTestSuite.T(), structUtilsTestSuite.inputStructJsonString, string(buffer))
}

func (structUtilsTestSuite *StructUtilsTestSuite) Test_StructUtils_NilInput_ToJsonReader_Success() {
	output := structUtilsTestSuite.structUtils.ToJsonReader(nil)
	assert.Nil(structUtilsTestSuite.T(), output)
}

func (structUtilsTestSuite *StructUtilsTestSuite) Test_StructUtils_NormalInput_ToJsonString_Success() {
	output := structUtilsTestSuite.structUtils.ToJsonString(structUtilsTestSuite.inputStruct)
	assert.Equal(structUtilsTestSuite.T(), structUtilsTestSuite.inputStructJsonString, *output)
}

func (structUtilsTestSuite *StructUtilsTestSuite) Test_StructUtils_NilInput_ToJsonString_Success() {
	output := structUtilsTestSuite.structUtils.ToJsonString(nil)
	assert.Nil(structUtilsTestSuite.T(), output)
}

func (structUtilsTestSuite *StructUtilsTestSuite) Test_StructUtils_NormalInput_FromString_Success() {
	output := structUtilsTestSuite.structUtils.FromJsonString(structUtilsTestSuite.inputStructJsonString,
		reflect.TypeOf(inputStruct{}))
	isDeepEqual := reflect.DeepEqual(output, &structUtilsTestSuite.inputStruct)
	assert.Equal(structUtilsTestSuite.T(), true, isDeepEqual)
}

func (structUtilsTestSuite *StructUtilsTestSuite) Test_StructUtils_PtrInput_FromString_Success() {
	output := structUtilsTestSuite.structUtils.FromJsonString(structUtilsTestSuite.inputStructJsonString,
		reflect.TypeOf(&inputStruct{}))
	isDeepEqual := reflect.DeepEqual(output, &structUtilsTestSuite.inputStruct)
	assert.Equal(structUtilsTestSuite.T(), true, isDeepEqual)
}

type CryptoUtilsTestSuite struct {
	suite.Suite
	cryptoUtils           CryptoUtils
	inputStructFmtString  string
	inputStructJsonString string
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestCryptoUtilsTestSuite(t *testing.T) {
	suite.Run(t, new(CryptoUtilsTestSuite))
}

func (cryptoUtilsTestSuite *CryptoUtilsTestSuite) SetupTest() {
	cryptoUtilsTestSuite.cryptoUtils = CryptoUtils{}
}

func (cryptoUtilsTestSuite *CryptoUtilsTestSuite) Test_CryptoUtils_NormalInput_ToSha256_Success() {
	input := "input-string"
	expectedSha256 := sha256.Sum256([]byte(input))
	output := cryptoUtilsTestSuite.cryptoUtils.ToSha256(input)
	assert.Equal(cryptoUtilsTestSuite.T(), expectedSha256, output)
}

func (cryptoUtilsTestSuite *CryptoUtilsTestSuite) Test_CryptoUtils_NormalInput_ToSha256String_Success() {
	input := "input-string"
	prefix := "prefix"
	suffix := "suffix"
	sha256Bytes := sha256.Sum256([]byte(input))
	expectedOutput := prefix + hex.EncodeToString(sha256Bytes[:]) + suffix
	output := cryptoUtilsTestSuite.cryptoUtils.ToSha256String(prefix, input, suffix)
	assert.Equal(cryptoUtilsTestSuite.T(), expectedOutput, output)
}

// General Utils Tests

func Test_ToPtr_Success(t *testing.T) {
	var input = "hello"
	output := ToPtr[string](input)
	assert.Equal(t, &input, output)
}

func Test_PtrTo_Success(t *testing.T) {
	var input = "hello"
	output := PtrTo[string](&input)
	assert.Equal(t, input, output)
}

func Test_PtrTo_Failure(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()
	PtrTo[string](nil)
}

func Test_GetIntegerFromString_Success(t *testing.T) {
	input := "55"
	expectedOutput := 55
	output, error := GetIntegerFromString(input)
	assert.Nil(t, error)
	assert.Equal(t, expectedOutput, output)
}

func Test_GetIntegerFromString_Failure(t *testing.T) {
	input := "55t"
	_, error := GetIntegerFromString(input)
	assert.NotNil(t, error)
}
