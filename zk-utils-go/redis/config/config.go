package config

type RedisConfig struct {
	Host        string `yaml:"host" env:"REDIS_HOST" env-description:"Database host"`
	Port        string `yaml:"port" env:"REDIS_PORT" env-description:"Database port"`
	ReadTimeout int    `yaml:"readTimeout"`
}
