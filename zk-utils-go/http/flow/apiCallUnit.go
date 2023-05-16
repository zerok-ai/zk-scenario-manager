package zkhttp

import (
	"time"
)

type Cookie struct {
	Name    string    `yaml:"name"`
	Value   string    `yaml:"value"`
	Expires time.Time `yaml:"expires"`
}

type ApiCallUnit struct {
	Url             string             `yaml:"url"`
	Method          *string            `yaml:"method"`
	RequestBody     *string            `yaml:"request"`
	ContentType     *string            `yaml:"contentType"`
	Headers         *map[string]string `yaml:"headers"`
	Cookies         *[]Cookie          `yaml:"cookies"`
	ExistingCookies *[]string          `yaml:"existingCookies"`
	Timeout         int                `yaml:"timeout"`
}

type _ApiCallUnitBuilder struct {
	ApiCallUnit ApiCallUnit
}

type ApiCallUnitBuilder struct{}

func (apiCallUnitBuilder ApiCallUnitBuilder) WithUrl(url string) _ApiCallUnitBuilder {
	return _ApiCallUnitBuilder{}.WithUrl(url)
}

func (_apiCallUnitBuilder _ApiCallUnitBuilder) WithUrl(url string) _ApiCallUnitBuilder {
	_apiCallUnitBuilder.ApiCallUnit.Url = url
	methodGet := "GET"
	_apiCallUnitBuilder.ApiCallUnit.Method = &methodGet
	return _apiCallUnitBuilder
}

func (_apiCallUnitBuilder _ApiCallUnitBuilder) Method(method string) _ApiCallUnitBuilder {
	_apiCallUnitBuilder.ApiCallUnit.Method = &method
	return _apiCallUnitBuilder
}

func (_apiCallUnitBuilder _ApiCallUnitBuilder) Request(request string) _ApiCallUnitBuilder {
	_apiCallUnitBuilder.ApiCallUnit.RequestBody = &request
	return _apiCallUnitBuilder
}

func (_apiCallUnitBuilder _ApiCallUnitBuilder) ContentType(contentType string) _ApiCallUnitBuilder {
	_apiCallUnitBuilder.ApiCallUnit.ContentType = &contentType
	return _apiCallUnitBuilder
}

func (_apiCallUnitBuilder _ApiCallUnitBuilder) Header(key string, value string) _ApiCallUnitBuilder {
	if _apiCallUnitBuilder.ApiCallUnit.Headers == nil {
		_apiCallUnitBuilder.ApiCallUnit.Headers = &map[string]string{}
	}
	(*_apiCallUnitBuilder.ApiCallUnit.Headers)[key] = value
	return _apiCallUnitBuilder
}

// func (_apiCallUnitBuilder _ApiCallUnitBuilder) Cookie(cookie http.Cookie) _ApiCallUnitBuilder{
// 	if _apiCallUnitBuilder.ApiCallUnit.Cookies == nil {
// 		_apiCallUnitBuilder.ApiCallUnit.Cookies = &list.List{}
// 	}
// 	 (*_apiCallUnitBuilder.ApiCallUnit.Cookies).PushBack(cookie)
// 	return _apiCallUnitBuilder;
// }

// func (_apiCallUnitBuilder _ApiCallUnitBuilder) Cookies(cookies []http.Cookie) _ApiCallUnitBuilder{
// 	if _apiCallUnitBuilder.ApiCallUnit.Cookies == nil {
// 		_apiCallUnitBuilder.ApiCallUnit.Cookies = &list.List{}
// 	}
// 	for _, element := range cookies {
//         (*_apiCallUnitBuilder.ApiCallUnit.Cookies).PushBack(element)
//     }
// 	return _apiCallUnitBuilder;
// }

// func (_apiCallUnitBuilder _ApiCallUnitBuilder) RawCookie(name string, value string, expires time.Time) _ApiCallUnitBuilder{
// 	cookie := http.Cookie{};
// 	cookie.Name = name;
// 	cookie.Value = value;
// 	cookie.Expires = expires;

// 	if _apiCallUnitBuilder.ApiCallUnit.Cookies == nil {
// 		_apiCallUnitBuilder.ApiCallUnit.Cookies = &list.List{}
// 	}
// 	 (*_apiCallUnitBuilder.ApiCallUnit.Cookies).PushBack(cookie)
// 	return _apiCallUnitBuilder;
// }

func (_apiCallUnitBuilder _ApiCallUnitBuilder) Headers(headers map[string]string) _ApiCallUnitBuilder {
	if _apiCallUnitBuilder.ApiCallUnit.Headers == nil {
		_apiCallUnitBuilder.ApiCallUnit.Headers = &map[string]string{}
	}
	for key, value := range headers {
		(*_apiCallUnitBuilder.ApiCallUnit.Headers)[key] = value
	}
	return _apiCallUnitBuilder
}

func (_apiCallUnitBuilder _ApiCallUnitBuilder) Build() ApiCallUnit {
	return _apiCallUnitBuilder.ApiCallUnit
}
